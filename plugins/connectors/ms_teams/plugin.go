package ms_teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"infini.sh/coco/modules/common"
	"infini.sh/coco/plugins/connectors"
	"infini.sh/framework/core/api"
	httprouter "infini.sh/framework/core/api/router"
	"infini.sh/framework/core/env"
	"infini.sh/framework/core/errors"
	"infini.sh/framework/core/global"
	"infini.sh/framework/core/kv"
	"infini.sh/framework/core/orm"
	"infini.sh/framework/core/queue"
	"infini.sh/framework/core/task"
	"infini.sh/framework/core/util"

	log "github.com/cihub/seelog"
)

const ConnectorTeams = "msteams"


type OAuthConfig struct {
	AuthURL        string   `config:"auth_url" json:"auth_url"`
	TokenURL       string   `config:"token_url" json:"token_url"`
	RedirectURL    string   `config:"redirect_url" json:"redirect_url"`
	ClientID       string   `config:"client_id" json:"client_id"`
	ClientSecret   string   `config:"client_secret" json:"client_secret"`
	Scopes         []string `config:"scopes" json:"scopes"` 
	UserAccessToken string  `config:"user_access_token" json:"user_access_token"`
}

type Config struct {
	AccessToken        string      `config:"access_token" json:"access_token"`
	RefreshToken       string      `config:"refresh_token" json:"refresh_token"`
	TokenExpiry        string      `config:"token_expiry" json:"token_expiry"`
	RefreshTokenExpiry string      `config:"refresh_token_expiry" json:"refresh_token_expiry"`
	Profile            util.MapStr `config:"profile" json:"profile"`
}

type Plugin struct {
	api.Handler
	Enabled     bool               `config:"enabled"`
	Queue       *queue.QueueConfig `config:"queue"`
	Interval    string             `config:"interval"`
	PageSize    int                `config:"page_size"`
	OAuthConfig *OAuthConfig       `config:"o_auth_config"`
	UserInfoURL string 
	BaseURL     string 
}


func (p *Plugin) Setup() {
	// Load config: connector.msteams -> p
	ok, err := env.ParseConfig("connector.msteams", &p)
	if ok && err != nil && global.Env().SystemConfig.Configs.PanicOnConfigError {
		panic(err)
	}

	p.BaseURL = "https://graph.microsoft.com/v1.0"
	p.UserInfoURL = p.BaseURL + "/me"

	if !p.Enabled {
		return
	}
	if p.PageSize <= 0 {
		p.PageSize = 100
	}
	if p.Queue == nil {
		p.Queue = &queue.QueueConfig{Name: "indexing_documents"}
	}
	p.Queue = queue.SmartGetOrInitConfig(p.Queue)

	// Reasonable defaults; allow connector settings to override in Start()
	if p.OAuthConfig == nil {
		p.OAuthConfig = &OAuthConfig{
			RedirectURL: "/connector/msteams/oauth_redirect",
		}
	}
	if p.UserInfoURL == "" {
		p.UserInfoURL = "https://graph.microsoft.com/v1.0/me"
	}
	if p.BaseURL == "" {
		p.BaseURL = "https://graph.microsoft.com/v1.0"
	}

	// UI routes
	api.HandleUIMethod(api.GET, "/connector/msteams/connect", p.connect, api.RequireLogin())
	api.HandleUIMethod(api.GET, "/connector/msteams/oauth_redirect", p.oAuthRedirect, api.RequireLogin())
}

func (p *Plugin) Start() error {
	if !p.Enabled {
		return nil
	}
	task.RegisterScheduleTask(task.ScheduleTask{
		ID:          util.GetUUID(),
		Group:       "connectors",
		Singleton:   true,
		Interval:    util.GetDurationOrDefault(p.Interval, time.Second*30).String(),
		Description: "indexing Microsoft Teams content",
		Task: func(ctx context.Context) {
			var conns []common.Connector
			q := orm.Query{Size: 1}
			q.Conds = orm.And(orm.Eq("id", ConnectorTeams))
			if err, _ := orm.SearchWithJSONMapper(&conns, &q); err != nil {
    			panic(errors.Errorf("invalid %s connector: %v", ConnectorTeams, err))
			}
			if len(conns) == 0 {
    			log.Debugf("Connector %s not found", ConnectorTeams)
    			return
			}
			conn := conns[0]

			// Pull OAuth endpoints/creds from connector.Config
			if conn.Config != nil {
				if p.OAuthConfig == nil {
					p.OAuthConfig = &OAuthConfig{}
				}
				if v, ok := conn.Config["auth_url"].(string); ok {
					p.OAuthConfig.AuthURL = v
				}
				if v, ok := conn.Config["token_url"].(string); ok {
					p.OAuthConfig.TokenURL = v
				}
				if v, ok := conn.Config["redirect_url"].(string); ok {
					p.OAuthConfig.RedirectURL = v
				}
				if v, ok := conn.Config["client_id"].(string); ok {
					p.OAuthConfig.ClientID = v
				}
				if v, ok := conn.Config["client_secret"].(string); ok {
					p.OAuthConfig.ClientSecret = v
				}
				if v, ok := conn.Config["scopes"].([]interface{}); ok {
					p.OAuthConfig.Scopes = make([]string, 0, len(v))
					for _, it := range v {
						if s, ok := it.(string); ok {
							p.OAuthConfig.Scopes = append(p.OAuthConfig.Scopes, s)
						}
					}
				}
				if v, ok := conn.Config["user_access_token"].(string); ok {
					p.OAuthConfig.UserAccessToken = v
				}
				if v, ok := conn.Config["user_info_url"].(string); ok && v != "" {
					p.UserInfoURL = v
				}
				if v, ok := conn.Config["base_url"].(string); ok && v != "" {
					p.BaseURL = v
				}
			}

			// Require either client_id or a service token
			if p.OAuthConfig == nil || (p.OAuthConfig.ClientID == "" && p.OAuthConfig.UserAccessToken == "") {
				log.Debugf("skipping %s connector task since no client_id or user_access_token configured", conn.ID)
				return
			}

			// Fetch syncable data sources
			q := orm.Query{Size: p.PageSize}
			q.Conds = orm.And(orm.Eq("connector.id", conn.ID), orm.Eq("sync.enabled", true))
			var results []common.DataSource
			if err, _ = orm.SearchWithJSONMapper(&results, &q); err != nil {
				panic(err)
			}

			for _, ds := range results {
				ok, err := connectors.CanDoSync(ds)
				if err != nil || !ok {
					continue
				}
				p.fetchTeamsGraph(&conn, &ds) // merged call
			}
		},
	})
	return nil
}

func (p *Plugin) Stop() error  { return nil }
func (p *Plugin) Name() string { return ConnectorTeams }

func init() {
	module.RegisterUserPlugin(&Plugin{})
}


func (p *Plugin) saveLastModifiedTime(datasourceID, lastModifiedTime string) error {
	bucket := fmt.Sprintf("/connector/%s/lastModifiedTime", ConnectorTeams)
	return kv.AddValue(bucket, []byte(datasourceID), []byte(lastModifiedTime))
}

func (p *Plugin) getLastModifiedTime(datasourceID string) (string, error) {
	bucket := fmt.Sprintf("/connector/%s/lastModifiedTime", ConnectorTeams)
	data, err := kv.GetValue(bucket, []byte(datasourceID))
	if err != nil {
		return "", err
	}
	return string(data), nil
}


func (p *Plugin) connect(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	if p.OAuthConfig == nil || p.OAuthConfig.ClientID == "" || p.OAuthConfig.ClientSecret == "" {
		http.Error(w, "OAuth not configured in connector. Please configure client_id and client_secret.", http.StatusServiceUnavailable)
		return
	}
	redirectURL := p.OAuthConfig.RedirectURL
	if !strings.HasPrefix(redirectURL, "http://") && !strings.HasPrefix(redirectURL, "https://") {
		scheme := "http"
		if req.TLS != nil || req.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		host := req.Host
		if host == "" {
			host = "localhost:8080"
		}
		redirectURL = fmt.Sprintf("%s://%s%s", scheme, host, redirectURL)
		p.OAuthConfig.RedirectURL = redirectURL
	}

	values := url.Values{}
	values.Set("client_id", p.OAuthConfig.ClientID)
	values.Set("response_type", "code")
	values.Set("redirect_uri", redirectURL)
	values.Set("response_mode", "query")
	if len(p.OAuthConfig.Scopes) > 0 {
		values.Set("scope", strings.Join(p.OAuthConfig.Scopes, " "))
	}
	authURL := fmt.Sprintf("%s?%s", p.OAuthConfig.AuthURL, values.Encode())

	log.Debugf("[%s connector] Redirecting to OAuth URL: %s", ConnectorTeams, authURL)
	http.Redirect(w, req, authURL, http.StatusTemporaryRedirect)
}

func (p *Plugin) oAuthRedirect(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	if p.OAuthConfig == nil || p.OAuthConfig.ClientID == "" || p.OAuthConfig.ClientSecret == "" {
		http.Error(w, "OAuth not configured in connector. Please configure client_id and client_secret.", http.StatusServiceUnavailable)
		return
	}

	code := req.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing authorization code.", http.StatusBadRequest)
		return
	}

	log.Debugf("[%s connector] Received authorization code", ConnectorTeams)

	token, err := p.exchangeCodeForToken(code)
	if err != nil {
		log.Errorf("[%s connector] Failed to exchange code for token: %v", ConnectorTeams, err)
		http.Error(w, "Failed to exchange authorization code for token.", http.StatusInternalServerError)
		return
	}

	profile, err := p.getUserProfile(token.AccessToken)
	if err != nil {
		log.Errorf("[%s connector] Failed to get user profile: %v", ConnectorTeams, err)
		http.Error(w, "Failed to get user profile information.", http.StatusInternalServerError)
		return
	}

	log.Infof("[%s connector] Successfully authenticated user: %v", ConnectorTeams, profile)

	ds := common.DataSource{
		SyncConfig: common.SyncConfig{Enabled: true, Interval: "30s"},
		Enabled:    true,
	}

	userID := util.ToString(profile["id"])
	if userID == "" {
		userID = util.ToString(profile["userPrincipalName"])
	}
	if userID == "" {
		userID = "unknown"
	}

	ds.ID = util.MD5digest(fmt.Sprintf("%v,%v", ConnectorTeams, userID))
	ds.Type = "connector"

	name := util.ToString(profile["displayName"])
	if name != "" {
		ds.Name = fmt.Sprintf("%s's Microsoft Teams", name)
	} else {
		ds.Name = "My Microsoft Teams"
	}

	ds.Connector = common.ConnectorConfig{
		ConnectorID: ConnectorTeams,
		Config: util.MapStr{
			"access_token":  token.AccessToken,
			"refresh_token": token.RefreshToken,
			"token_expiry":  time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).Format(time.RFC3339),
			"profile":       profile,
		},
	}

	ctx := orm.NewContextWithParent(req.Context())
	if err := orm.Save(ctx, &ds); err != nil {
		log.Errorf("[%s connector] Failed to save datasource: %v", ConnectorTeams, err)
		http.Error(w, "Failed to save datasource.", http.StatusInternalServerError)
		return
	}

	log.Infof("[%s connector] Successfully created datasource: %s", ConnectorTeams, ds.ID)
	http.Redirect(w, req, fmt.Sprintf("/#/data-source/detail/%v", ds.ID), http.StatusTemporaryRedirect)
}


type Token struct {
	TokenType        string `json:"token_type"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int    `json:"expires_in"`
	Scope            string `json:"scope"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (p *Plugin) exchangeCodeForToken(code string) (*Token, error) {
	if p.OAuthConfig == nil {
		return nil, errors.Errorf("OAuth config not initialized")
	}
	payload := url.Values{}
	payload.Set("client_id", p.OAuthConfig.ClientID)
	payload.Set("client_secret", p.OAuthConfig.ClientSecret)
	payload.Set("grant_type", "authorization_code")
	payload.Set("code", code)
	payload.Set("redirect_uri", p.OAuthConfig.RedirectURL)
	if len(p.OAuthConfig.Scopes) > 0 {
		payload.Set("scope", strings.Join(p.OAuthConfig.Scopes, " "))
	}

	req := util.NewPostRequest(p.OAuthConfig.TokenURL, []byte(payload.Encode()))
	req.AddHeader("Content-Type", "application/x-www-form-urlencoded")

	res, err := util.ExecuteRequest(req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.Errorf("%s API error, no response", ConnectorTeams)
	}
	if res.StatusCode >= 300 {
		return nil, errors.Errorf("%s API error: status %d, body: %s", ConnectorTeams, res.StatusCode, string(res.Body))
	}

	var tokenResponse Token
	if err := json.Unmarshal(res.Body, &tokenResponse); err != nil {
		return nil, err
	}
	return &tokenResponse, nil
}

func (p *Plugin) refreshAccessToken(refreshToken string) (*Token, error) {
	if p.OAuthConfig == nil {
		return nil, errors.Errorf("OAuth config not initialized")
	}
	payload := url.Values{}
	payload.Set("client_id", p.OAuthConfig.ClientID)
	payload.Set("client_secret", p.OAuthConfig.ClientSecret)
	payload.Set("grant_type", "refresh_token")
	payload.Set("refresh_token", refreshToken)

	req := util.NewPostRequest(p.OAuthConfig.TokenURL, []byte(payload.Encode()))
	req.AddHeader("Content-Type", "application/x-www-form-urlencoded")

	res, err := util.ExecuteRequest(req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.Errorf("%s API error, no response", ConnectorTeams)
	}
	if res.StatusCode >= 300 {
		return nil, errors.Errorf("%s API error: status %d, body: %s", ConnectorTeams, res.StatusCode, string(res.Body))
	}

	var tokenResponse Token
	if err := json.Unmarshal(res.Body, &tokenResponse); err != nil {
		return nil, err
	}
	return &tokenResponse, nil
}

func (p *Plugin) getUserProfile(accessToken string) (util.MapStr, error) {
	req := util.NewGetRequest(p.UserInfoURL, nil)
	req.AddHeader("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	res, err := util.ExecuteRequest(req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.Errorf("%s API error, no response", ConnectorTeams)
	}
	if res.StatusCode >= 300 {
		return nil, errors.Errorf("%s API error: status %d, body: %s", ConnectorTeams, res.StatusCode, string(res.Body))
	}

	var out util.MapStr
	if err := json.Unmarshal(res.Body, &out); err != nil {
		return nil, err
	}
	return out, nil
}


type SyncContext struct {
	Token      string
	PageSize   int
	DataSource *common.DataSource
	LastKnown  time.Time
	LatestSeen *time.Time

	TeamID    string
	ChannelID string
}

func (p *Plugin) fetchTeamsGraph(connector *common.Connector, ds *common.DataSource) {
	if connector == nil || ds == nil {
		panic("invalid connector config")
	}

	cfg, err := util.NewConfigFrom(ds.Connector.Config)
	if err != nil {
		panic(err)
	}
	var obj Config
	if err := cfg.Unpack(&obj); err != nil {
		panic(err)
	}

	token := strings.TrimSpace(obj.AccessToken)
	if token == "" && p.OAuthConfig != nil && p.OAuthConfig.UserAccessToken != "" {
		token = strings.TrimSpace(p.OAuthConfig.UserAccessToken)
	}
	if token == "" {
		_ = log.Errorf("[%s connector] missing access token for datasource [%s]", ConnectorTeams, ds.Name)
		return
	}

	if obj.AccessToken != "" && obj.TokenExpiry != "" {
		if exp, err := time.Parse(time.RFC3339, obj.TokenExpiry); err == nil && time.Now().After(exp) && obj.RefreshToken != "" {
			newTok, err := p.refreshAccessToken(obj.RefreshToken)
			if err != nil {
				_ = log.Errorf("[%s connector] failed to refresh token: %v", ConnectorTeams, err)
			} else {
				obj.AccessToken = newTok.AccessToken
				obj.RefreshToken = newTok.RefreshToken
				obj.TokenExpiry = time.Now().Add(time.Duration(newTok.ExpiresIn) * time.Second).Format(time.RFC3339)

				ds.Connector.Config = obj
				ctx := orm.NewContext().DirectAccess()
				if err := orm.Update(ctx, ds); err != nil {
					_ = log.Errorf("[%s connector] failed to save refreshed token: %v", ConnectorTeams, err)
				}
				token = obj.AccessToken
			}
		}
	}

	var lastKnown time.Time
	if lastStr, _ := p.getLastModifiedTime(ds.ID); lastStr != "" {
		if t, err := time.Parse(time.RFC3339, lastStr); err == nil {
			lastKnown = t.Add(-1 * time.Minute)
		}
	}
	var latestSeen time.Time

	ctx := &SyncContext{
		Token:      token,
		PageSize:   max(p.PageSize, 50),
		DataSource: ds,
		LastKnown:  lastKnown,
		LatestSeen: &latestSeen,
	}
	p.enumerateTeams(ctx)

	if !latestSeen.IsZero() {
		_ = p.saveLastModifiedTime(ds.ID, latestSeen.UTC().Format(time.RFC3339))
	}
	log.Infof("[%s connector] sync completed for datasource: ID: %s, Name: %s", ConnectorTeams, ds.ID, ds.Name)
}

func (p *Plugin) enumerateTeams(ctx *SyncContext) {
    url := fmt.Sprintf("%s/me/joinedTeams?$top=%d", p.BaseURL, ctx.PageSize)
    for url != "" {
        body, next, _ := p.getPaged(ctx.Token, url)
        teams := parseTeams(body)
        for _, t := range teams {
            folderDoc := common.CreateHierarchyPathFolderDoc(
                ctx.DataSource, t.ID, t.DisplayName, []string{},
            )

            p.push(folderDoc)
            p.enumerateChannels(ctx, t.ID, t.DisplayName)
        }
        url = next
    }
}

func (p *Plugin) enumerateChannels(ctx *SyncContext, teamID, teamName string) {
    url := fmt.Sprintf("%s/teams/%s/channels?$top=%d", p.BaseURL, teamID, ctx.PageSize)
    for url != "" {
        body, next, _ := p.getPaged(ctx.Token, url)
        chs := parseChannels(body)
        for _, ch := range chs {
            folderDoc := common.CreateHierarchyPathFolderDoc(
                ctx.DataSource, ch.ID, ch.DisplayName, []string{teamName},
            )
            p.push(folderDoc)
            p.enumerateMessages(ctx, teamID, ch.ID, teamName, ch.DisplayName)
            p.enumerateChannelFiles(ctx, teamID, ch.ID, teamName, ch.DisplayName)
        }
        url = next
    }
}

func (p *Plugin) enumerateMessages(ctx *SyncContext, teamID, channelID, teamName, channelName string) {
    url := fmt.Sprintf("%s/teams/%s/channels/%s/messages?$top=%d&$orderby=lastModifiedDateTime desc",
        p.BaseURL, teamID, channelID, ctx.PageSize)

    for url != "" {
        body, next, _ := p.getPaged(ctx.Token, url)
        msgs := parseMessages(body)

        for i, m := range msgs {
            updatedAt := maxTime(m.LastModified, m.Created)

            if !ctx.LastKnown.IsZero() && !updatedAt.IsZero() && !updatedAt.After(ctx.LastKnown) {
                continue
            }

            if ctx.LatestSeen.IsZero() || updatedAt.After(*ctx.LatestSeen) {
                *ctx.LatestSeen = updatedAt
            }

            doc := buildMessageDoc(ctx, m, []string{teamName, channelName}, i)
            p.push(doc)

            if m.HasReplies {
                repliesURL := fmt.Sprintf("%s/teams/%s/channels/%s/messages/%s/replies?$top=%d&$orderby=lastModifiedDateTime desc",
                    p.BaseURL, teamID, channelID, m.ID, ctx.PageSize)
                for repliesURL != "" {
                    replyBody, nextReplies, _ := p.getPaged(ctx.Token, repliesURL)
                    replies := parseMessages(replyBody)
                    for _, r := range replies {
                        replyUpdated := maxTime(r.LastModified, r.Created)
                        if !ctx.LastKnown.IsZero() && !replyUpdated.IsZero() && !replyUpdated.After(ctx.LastKnown) {
                            continue
                        }
                        if ctx.LatestSeen.IsZero() || replyUpdated.After(*ctx.LatestSeen) {
                            *ctx.LatestSeen = replyUpdated
                        }
                        replyDoc := buildMessageDoc(ctx, r, []string{teamName, channelName, "reply"}, 0)
                        p.push(replyDoc)
                    }
                    repliesURL = nextReplies
                }
            }

            attachmentsURL := fmt.Sprintf("%s/teams/%s/channels/%s/messages/%s/attachments",
                p.BaseURL, teamID, channelID, m.ID)
            attachmentsBody, _, _ := p.getPaged(ctx.Token, attachmentsURL)
            attachments := parseAttachments(attachmentsBody)
            for _, a := range attachments {
                fileDoc := buildAttachmentDoc(ctx, a, []string{teamName, channelName})
                p.push(fileDoc)
            }
        }

        url = next
    }
}


func max(a, b int) int { if a > b { return a }; return b }
