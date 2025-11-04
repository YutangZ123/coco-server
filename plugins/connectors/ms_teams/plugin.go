package ms_teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"io"

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
	"infini.sh/framework/core/module"

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

			var err error

			// Fetch syncable data sources
			q = orm.Query{Size: p.PageSize}
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

	var obj Config
	if ds != nil && ds.Connector.Config != nil {
    	b, _ := json.Marshal(ds.Connector.Config) // import "encoding/json"
    	if err := json.Unmarshal(b, &obj); err != nil {
        	panic(err)
    	}
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

// Teams → Channels → Messages (+Replies, +Attachments)

func (p *Plugin) enumerateTeams(ctx *SyncContext) {
    if ctx == nil || ctx.DataSource == nil {
        log.Errorf("[%s] enumerateTeams: nil context/datasource", ConnectorTeams)
        return
    }
    url := fmt.Sprintf("%s/me/joinedTeams?$top=%d", p.BaseURL, ctx.PageSize)

    for url != "" {
        body, next, err := p.getPaged(ctx.Token, url)
        if err != nil {
            log.Errorf("[%s] enumerateTeams: %v", ConnectorTeams, err)
            break
        }

        // NOTE: If your parseTeams returns ([]graphTeam, error), use the two-value form:
        teams := parseTeams(body)

        for _, t := range teams {
            // Emit a folder doc for the Team itself (root level)
            teamFolder := common.CreateHierarchyPathFolderDoc(
                ctx.DataSource, t.ID, t.DisplayName, []string{},
            )
            if err := p.push(&teamFolder); err != nil {
                log.Warnf("[%s] push team folder (%s): %v", ConnectorTeams, t.DisplayName, err)
            }

            // Dive into channels for this team
            p.enumerateChannels(ctx, t.ID, t.DisplayName)
        }
        url = next
    }
}

func (p *Plugin) enumerateChannels(ctx *SyncContext, teamID, teamName string) {
    if ctx == nil || ctx.DataSource == nil {
        log.Errorf("[%s] enumerateChannels: nil context/datasource", ConnectorTeams)
        return
    }
    if teamID == "" {
        log.Warnf("[%s] enumerateChannels: empty teamID for team %q", ConnectorTeams, teamName)
        return
    }

    url := fmt.Sprintf("%s/teams/%s/channels?$top=%d", p.BaseURL, teamID, ctx.PageSize)

    for url != "" {
        body, next, err := p.getPaged(ctx.Token, url)
        if err != nil {
            log.Errorf("[%s] enumerateChannels(%s): %v", ConnectorTeams, teamName, err)
            break
        }

        // If your parseChannels returns ([]graphChannel, error), use the two-value form:
        chs := parseChannels(body)

        for _, ch := range chs {
            // Emit a folder doc for Team/Channel
            channelFolder := common.CreateHierarchyPathFolderDoc(
                ctx.DataSource, ch.ID, ch.DisplayName, []string{teamName},
            )
            if err := p.push(&channelFolder); err != nil {
                log.Warnf("[%s] push channel folder (%s/%s): %v", ConnectorTeams, teamName, ch.DisplayName, err)
            }

            // Messages + Files for this channel
            p.enumerateMessages(ctx, teamID, ch.ID, teamName, ch.DisplayName)
            p.enumerateChannelFiles(ctx, teamID, ch.ID, teamName, ch.DisplayName)
        }
        url = next
    }
}

type driveItem struct {
    ID                   string    `json:"id"`
    Name                 string    `json:"name"`
    WebURL               string    `json:"webUrl"`
    Size                 int64     `json:"size"`
    LastModifiedDateTime time.Time `json:"lastModifiedDateTime"`

    LastModifiedBy struct {
        User struct {
            DisplayName string `json:"displayName"`
            ID          string `json:"id"`
        } `json:"user"`
    } `json:"lastModifiedBy"`

    File   *struct{ MimeType string `json:"mimeType"` } `json:"file,omitempty"`
    Folder *struct {
        ChildCount int `json:"childCount"`
    } `json:"folder,omitempty"`

    ParentReference struct {
        DriveID string `json:"driveId"`
    } `json:"parentReference"`
}

type filesFolderResp struct {
    ID              string `json:"id"`
    Name            string `json:"name"`
    WebURL          string `json:"webUrl"`
    ParentReference struct {
        DriveID string `json:"driveId"`
    } `json:"parentReference"`
}

// --- public entrypoint called from enumerateChannels ---
func (p *Plugin) enumerateChannelFiles(ctx *SyncContext, teamID, channelID, teamName, channelName string) {
    if ctx == nil || ctx.DataSource == nil {
        log.Errorf("[%s] enumerateChannelFiles: nil context/datasource", ConnectorTeams)
        return
    }
    if teamID == "" || channelID == "" {
        log.Warnf("[%s] enumerateChannelFiles: missing teamID/channelID for %q/%q", ConnectorTeams, teamName, channelName)
        return
    }

    // 1) Locate the channel’s filesFolder to learn driveId + root item id
    filesFolderURL := fmt.Sprintf("%s/teams/%s/channels/%s/filesFolder", p.BaseURL, teamID, channelID)
    root, err := p.getFilesFolder(ctx.Token, filesFolderURL)
    if err != nil {
        log.Warnf("[%s] filesFolder(%s/%s): %v", ConnectorTeams, teamName, channelName, err)
        return
    }
    if root.ParentReference.DriveID == "" || root.ID == "" {
        log.Warnf("[%s] filesFolder missing driveId/item id for %q/%q", ConnectorTeams, teamName, channelName)
        return
    }

    // 2) Walk children under the filesFolder root
    basePath := []string{teamName, channelName}
    p.walkDriveChildren(ctx, root.ParentReference.DriveID, root.ID, basePath)
}

// --- helper: GET /teams/{team-id}/channels/{channel-id}/filesFolder ---
func (p *Plugin) getFilesFolder(token, url string) (*filesFolderResp, error) {
    req, err := http.NewRequest("GET", url, nil)
    if err != nil { return nil, err }
    req.Header.Set("Authorization", "Bearer "+token)
    req.Header.Set("Accept", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()

    b, err := io.ReadAll(resp.Body)
    if err != nil { return nil, err }
    if resp.StatusCode < 200 || resp.StatusCode > 299 {
        return nil, fmt.Errorf("GET %s: %d %s", url, resp.StatusCode, string(b))
    }

    var out filesFolderResp
    if err := json.Unmarshal(b, &out); err != nil {
        return nil, err
    }
    return &out, nil
}

// --- helper: recursively walk /drives/{driveId}/items/{itemId}/children ---
func (p *Plugin) walkDriveChildren(ctx *SyncContext, driveID, itemID string, path []string) {
    listURL := fmt.Sprintf("%s/drives/%s/items/%s/children?$top=%d", p.BaseURL, driveID, itemID, ctx.PageSize)

    for listURL != "" {
        body, next, err := p.getPaged(ctx.Token, listURL)
        if err != nil {
            log.Errorf("[%s] list drive children: %v", ConnectorTeams, err)
            break
        }

        // Parse page
        var page struct {
            Value []driveItem `json:"value"`
        }
        _ = json.Unmarshal(body, &page)

        for _, it := range page.Value {
            // Folders → emit folder doc and recurse
            if it.Folder != nil {
                folderDoc := common.CreateHierarchyPathFolderDoc(
                    ctx.DataSource, it.ID, it.Name, path,
                )
                if err := p.push(&folderDoc); err != nil {
                    log.Warnf("[%s] push folder %q: %v", ConnectorTeams, it.Name, err)
                }
                // Recurse into folder
                p.walkDriveChildren(ctx, driveID, it.ID, append(path, it.Name))
                continue
            }

            // Files → emit file doc
            if it.File != nil {
                // Build a Document that uses only fields available in your common.Document
                // (Title, Content, URL, Type, Category/Subcategory/Categories, Tags, Owner/LastUpdatedBy)
                var category, subcategory string
                var cats []string
                if len(path) > 0 {
                    category = path[0]
                    cats = append(cats, path...)
                    if len(path) > 1 {
                        subcategory = path[1]
                    }
                }

                // Owner/LastUpdated from drive item (if present)
                owner := &common.UserInfo{
                    UserName: it.LastModifiedBy.User.DisplayName,
                    UserID:   it.LastModifiedBy.User.ID,
                }
                upd := it.LastModifiedDateTime
                var lastUpd *common.EditorInfo
                if !upd.IsZero() || owner.UserName != "" || owner.UserID != "" {
                    lastUpd = &common.EditorInfo{UserInfo: owner, UpdatedAt: &upd}
                } else {
                    owner = nil
                }

                // Source reference (no .ID field on your DataSource: use Name as a stable identifier)
                src := common.DataSourceReference{
                    Type: ConnectorTeams,
                    Name: ctx.DataSource.Name,
                    ID:   ctx.DataSource.Name, // use Name here since common.DataSource may not expose ID
                }

                doc := &common.Document{
                    Source:        src,
                    Type:          "file",
                    Title:         it.Name,
                    Content:       "",           // optional; can leave empty for files
                    URL:           it.WebURL,    // SharePoint/Graph webUrl
                    Category:      category,
                    Subcategory:   subcategory,
                    Categories:    cats,
                    Owner:         owner,
                    LastUpdatedBy: lastUpd,
                    Tags:          []string{"teams", "channel", "file"},
                    // Size: int(it.Size) // ← only set this if your Document.Size is int (it is), else omit or cast carefully
                }

                if err := p.push(doc); err != nil {
                    log.Warnf("[%s] push file %q: %v", ConnectorTeams, it.Name, err)
                }
            }
        }

        listURL = next
    }
}


func (p *Plugin) enumerateMessages(ctx *SyncContext, teamID, channelID, teamName, channelName string) {
    if ctx == nil || ctx.DataSource == nil {
        log.Errorf("[%s] enumerateMessages: nil context/datasource", ConnectorTeams)
        return
    }
    if teamID == "" || channelID == "" {
        log.Warnf("[%s] enumerateMessages: empty teamID/channelID for %s/%s", ConnectorTeams, teamName, channelName)
        return
    }

    // Order by lastModified desc so we can short-circuit early pages in the future if needed
    url := fmt.Sprintf("%s/teams/%s/channels/%s/messages?$top=%d&$orderby=lastModifiedDateTime desc",
        p.BaseURL, teamID, channelID, ctx.PageSize)

    for url != "" {
        body, next, err := p.getPaged(ctx.Token, url)
        if err != nil {
            log.Errorf("[%s] enumerateMessages(%s/%s): %v", ConnectorTeams, teamName, channelName, err)
            break
        }

        msgs := parseMessages(body) // this helper is no-error; adjust if your signature differs
        for i, m := range msgs {
            // Incremental gating
            updatedAt := maxTime(m.LastModified, m.Created)
            if !ctx.LastKnown.IsZero() && !updatedAt.IsZero() && !updatedAt.After(ctx.LastKnown) {
                continue
            }
            // Track watermark
            if ctx.LatestSeen.IsZero() || updatedAt.After(*ctx.LatestSeen) {
                *ctx.LatestSeen = updatedAt
            }

            // Build + push message doc
            msgDoc := buildMessageDoc(ctx, m, []string{teamName, channelName}, i)
            if err := p.push(msgDoc); err != nil {
                log.Warnf("[%s] push message doc (%s/%s): %v", ConnectorTeams, teamName, channelName, err)
            }

            // Replies (optional)
            if m.HasReplies {
                repliesURL := fmt.Sprintf("%s/teams/%s/channels/%s/messages/%s/replies?$top=%d&$orderby=lastModifiedDateTime desc",
                    p.BaseURL, teamID, channelID, m.ID, ctx.PageSize)
                for repliesURL != "" {
                    replyBody, nextReplies, rerr := p.getPaged(ctx.Token, repliesURL)
                    if rerr != nil {
                        log.Errorf("[%s] enumerateReplies(%s/%s): %v", ConnectorTeams, teamName, channelName, rerr)
                        break
                    }
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
                        if err := p.push(replyDoc); err != nil {
                            log.Warnf("[%s] push reply doc (%s/%s): %v", ConnectorTeams, teamName, channelName, err)
                        }
                    }
                    repliesURL = nextReplies
                }
            }

            // Attachments (optional)
            attachmentsURL := fmt.Sprintf("%s/teams/%s/channels/%s/messages/%s/attachments",
                p.BaseURL, teamID, channelID, m.ID)
            attBody, _, aerr := p.getPaged(ctx.Token, attachmentsURL)
            if aerr == nil && len(attBody) > 0 {
                atts := parseAttachments(attBody)
                for _, a := range atts {
                    attDoc := buildAttachmentDoc(ctx, a, []string{teamName, channelName})
                    if err := p.push(attDoc); err != nil {
                        log.Warnf("[%s] push attachment doc (%s/%s): %v", ConnectorTeams, teamName, channelName, err)
                    }
                }
            }
        }

        url = next
    }
}


func max(a, b int) int { if a > b { return a }; return b }

// Graph entity structs
type graphTeam struct {
    ID          string `json:"id"`
    DisplayName string `json:"displayName"`
}

type graphChannel struct {
    ID          string `json:"id"`
    DisplayName string `json:"displayName"`
}

type graphMessage struct {
    ID           string    `json:"id"`
    From         struct {
        User struct {
            DisplayName string `json:"displayName"`
            ID          string `json:"id"`
        } `json:"user"`
    } `json:"from"`
    Body struct {
        Content string `json:"content"`
    } `json:"body"`
    Created      time.Time `json:"createdDateTime"`
    LastModified time.Time `json:"lastModifiedDateTime"`
    HasReplies   bool      `json:"hasReplies"`
}

type graphAttachment struct {
    ID          string `json:"id"`
    Name        string `json:"name"`
    ContentType string `json:"contentType"`
    ContentURL  string `json:"contentUrl"`
}

// getPaged issues a GET request with a bearer token and returns body + next link.
func (p *Plugin) getPaged(token, url string) (body []byte, next string, err error) {
    req, err := http.NewRequest("GET", url, nil)
    if err != nil {
        return nil, "", err
    }
    req.Header.Set("Authorization", "Bearer "+token)
    req.Header.Set("Accept", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, "", err
    }
    defer resp.Body.Close()

    b, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, "", err
    }
    if resp.StatusCode < 200 || resp.StatusCode > 299 {
        return nil, "", fmt.Errorf("GET %s: %d %s", url, resp.StatusCode, string(b))
    }

    var probe struct {
        Next string `json:"@odata.nextLink"`
    }
    _ = json.Unmarshal(b, &probe)
    return b, probe.Next, nil
}

// Simple JSON decoders for Teams entities
func parseTeams(b []byte) []graphTeam {
    var out struct {
        Value []graphTeam `json:"value"`
    }
    _ = json.Unmarshal(b, &out)
    return out.Value
}

func parseChannels(b []byte) []graphChannel {
    var out struct {
        Value []graphChannel `json:"value"`
    }
    _ = json.Unmarshal(b, &out)
    return out.Value
}

func parseMessages(b []byte) []graphMessage {
    var out struct {
        Value []graphMessage `json:"value"`
    }
    _ = json.Unmarshal(b, &out)
    return out.Value
}

func parseAttachments(b []byte) []graphAttachment {
    var out struct {
        Value []graphAttachment `json:"value"`
    }
    _ = json.Unmarshal(b, &out)
    return out.Value
}

// push enqueues a document to the indexing queue
func (p *Plugin) push(doc *common.Document) error {
    if doc == nil {
        return nil
    }
    qc := queue.SmartGetOrInitConfig(p.Queue)
	payload, err := json.Marshal(doc)
    if err != nil { return err }
    return queue.Push(qc, payload)
}

// Helpers to construct Document objects for messages and attachments
func buildMessageDoc(ctx *SyncContext, m graphMessage, path []string, idx int) *common.Document {
    title := m.From.User.DisplayName
    content := m.Body.Content
    if strings.TrimSpace(content) == "" {
        content = "(empty message)"
    }

    // Choose an "updated" timestamp: prefer LastModified, fallback to Created
    updated := m.LastModified
    if updated.IsZero() {
        updated = m.Created
    }

    // Owner and LastUpdatedBy (optional but valid)
    owner := &common.UserInfo{
        UserName: m.From.User.DisplayName,
        UserID:   m.From.User.ID,
    }
    lastUpd := &common.EditorInfo{
        UserInfo:  owner,
        UpdatedAt: &updated,
    }

    // Source reference must be a DataSourceReference (not a string)
    src := common.DataSourceReference{
        Type: ConnectorTeams,        // e.g., "msteams"
        Name: ctx.DataSource.Name,   // datasource friendly name
        ID:   ctx.DataSource.Name,     // datasource id
        // Icon optional: leave empty or set if you have one
    }

    // Optional categorization: use team/channel path as categories
    // (Top-level Category/Subcategory plus Categories[] for full path)
    var category, subcategory string
    var cats []string
    if len(path) > 0 {
        category = path[0]
        cats = append(cats, path...)
        if len(path) > 1 {
            subcategory = path[1]
        }
    }

    return &common.Document{
        Source:        src,
        Type:          "message",
        Title:         title,
        Content:       content,
        Category:      category,
        Subcategory:   subcategory,
        Categories:    cats,
        Owner:         owner,
        LastUpdatedBy: lastUpd,
        Tags:          []string{"teams", "channel", "message"},
        // URL: fill with a deep link if you have one
    }
}

func buildAttachmentDoc(ctx *SyncContext, a graphAttachment, path []string) *common.Document {
    // Similar categorization using path
    var category, subcategory string
    var cats []string
    if len(path) > 0 {
        category = path[0]
        cats = append(cats, path...)
        if len(path) > 1 {
            subcategory = path[1]
        }
    }

    src := common.DataSourceReference{
        Type: ConnectorTeams,
        Name: ctx.DataSource.Name,
        ID:   ctx.DataSource.Name,
    }

    return &common.Document{
        Source:      src,
        Type:        "attachment",
        Title:       a.Name,
        Content:     fmt.Sprintf("Attachment: %s (%s)", a.Name, a.ContentType),
        URL:         a.ContentURL, // direct link if accessible; ok to leave empty otherwise
        Category:    category,
        Subcategory: subcategory,
        Categories:  cats,
        Tags:        []string{"teams", "channel", "attachment"},
    }
}

// Safely compute the most recent time between two timestamps
func maxTime(a, b time.Time) time.Time {
    if a.After(b) {
        return a
    }
    return b
}
