package microsoft_teams

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
	Scopes         []string `config:"scopes" json:"scopes"` // e.g. ["User.Read","Group.Read.All","Channel.ReadBasic.All","ChannelMessage.Read.All","offline_access"]
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
	UserInfoURL string // e.g. https://graph.microsoft.com/v1.0/me
	BaseURL     string // e.g. https://graph.microsoft.com/v1.0
}

// --------------------------
// Lifecycle (merged Setup+Start)
// --------------------------

func (p *Plugin) Setup() {
	// Load config: connector.msteams -> p (same pattern as Feishu)
	ok, err := env.ParseConfig("connector.msteams", &p)
	if ok && err != nil && global.Env().SystemConfig.Configs.PanicOnConfigError {
		panic(err)
	}

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

	// Ensure OAuthConfig exists (redirect path is local; host/scheme normalized in connect)
	if p.OAuthConfig == nil {
		p.OAuthConfig = &OAuthConfig{
			RedirectURL: "/connector/msteams/oauth_redirect",
		}
	}

	// ---------- NEW: Auto-select Graph endpoints ----------
	// 1) If admin set an explicit region in config, prefer that.
	//    Example config:
	//    connector.msteams:
	//      region: "global" | "gov" | "cn"
	region := strings.ToLower(env.GetString("connector.msteams.region"))

	// 2) If region is not set, try to infer from OAuth endpoints (AuthURL/TokenURL).
	inferFrom := ""
	if p.OAuthConfig != nil {
		if p.OAuthConfig.AuthURL != "" {
			inferFrom = p.OAuthConfig.AuthURL
		} else if p.OAuthConfig.TokenURL != "" {
			inferFrom = p.OAuthConfig.TokenURL
		}
	}

	var base string
	switch {
	case region == "cn" || region == "china" ||
		(inferFrom != "" && strings.Contains(inferFrom, "chinacloudapi.cn")):
		base = "https://microsoftgraph.chinacloudapi.cn/v1.0"
	case region == "gov" || region == "usgov" ||
		(inferFrom != "" && (strings.Contains(inferFrom, "microsoft.us") || strings.Contains(inferFrom, "dod-us"))):
		base = "https://graph.microsoft.us/v1.0"
	default:
		base = "https://graph.microsoft.com/v1.0"
	}

	// Only set if not already explicitly configured.
	if p.BaseURL == "" {
		p.BaseURL = base
	}
	if p.UserInfoURL == "" {
		p.UserInfoURL = strings.TrimRight(p.BaseURL, "/") + "/me"
	}
	// ---------- END: Auto-select Graph endpoints ----------

	// UI routes (merged here instead of separate file)
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
			conn := common.Connector{ID: ConnectorTeams}
			exists, err := orm.Get(&conn)
			if !exists {
				log.Debugf("Connector %s not found", conn.ID)
				return
			}
			if err != nil {
				panic(errors.Errorf("invalid %s connector:%v", conn.ID, err))
			}

			// Pull OAuth endpoints/creds from connector.Config (like Feishu):contentReference[oaicite:7]{index=7}
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
				// Optional: override graph base URLs
				if v, ok := conn.Config["user_info_url"].(string); ok && v != "" {
					p.UserInfoURL = v
				}
				if v, ok := conn.Config["base_url"].(string); ok && v != "" {
					p.BaseURL = v
				}
			}

			// Require either client_id or a service token (same check as Feishu):contentReference[oaicite:8]{index=8}
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
	// Register like Feishu’s init() does for discovery:contentReference[oaicite:9]{index=9}
	module.RegisterUserPlugin(&Plugin{})
}

// --------------------------
// KV helpers (incremental)
// --------------------------

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

// --------------------------
// OAuth UI (merged connect + redirect)
// --------------------------

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

// --------------------------
// Token & profile helpers
// --------------------------

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

// --------------------------
// Sync (Teams -> Channels -> Messages)
// --------------------------

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

	// Load ds tokens; refresh if needed (same pattern as Feishu):contentReference[oaicite:10]{index=10}
	cfg, err := util.NewConfigFrom(ds.Connector.Config) // accepts util.MapStr
	if err != nil {
		panic(err)
	}
	var obj Config
	if err := cfg.Unpack(&obj); err != nil {
		panic(err)
	}

	// Choose token: prefer OAuth tokens, else service token from connector config:contentReference[oaicite:11]{index=11}:contentReference[oaicite:12]{index=12}
	token := strings.TrimSpace(obj.AccessToken)
	if token == "" && p.OAuthConfig != nil && p.OAuthConfig.UserAccessToken != "" {
		token = strings.TrimSpace(p.OAuthConfig.UserAccessToken)
	}
	if token == "" {
		_ = log.Errorf("[%s connector] missing access token for datasource [%s]", ConnectorTeams, ds.Name)
		return
	}

	// Refresh if expired (mirror Feishu pattern):contentReference[oaicite:13]{index=13}
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

	// Incremental watermark (same KV pattern as Feishu):contentReference[oaicite:14]{index=14}
	var lastKnown time.Time
	if lastStr, _ := p.getLastModifiedTime(ds.ID); lastStr != "" {
		if t, err := time.Parse(time.RFC3339, lastStr); err == nil {
			lastKnown = t.Add(-1 * time.Minute) // buffer like Feishu:contentReference[oaicite:15]{index=15}
		}
	}
	var latestSeen time.Time

	// Enumerate: Teams -> Channels -> Messages (stubs; implement Graph calls as needed)
	ctx := &SyncContext{
		Token:      token,
		PageSize:   max(p.PageSize, 50),
		DataSource: ds,
		LastKnown:  lastKnown,
		LatestSeen: &latestSeen,
	}
	p.enumerateTeams(ctx) // -> enumerateChannels -> enumerateMessages

	// Save watermark
	if !latestSeen.IsZero() {
		_ = p.saveLastModifiedTime(ds.ID, latestSeen.UTC().Format(time.RFC3339))
	}
	log.Infof("[%s connector] sync completed for datasource: ID: %s, Name: %s", ConnectorTeams, ds.ID, ds.Name)
}

func (p *Plugin) enumerateTeams(ctx *SyncContext) {
	// GET {BaseURL}/me/joinedTeams?$top=... (handle paging)
	// For each team -> create folder doc (Team) and call enumerateChannels(...)
}

func (p *Plugin) enumerateChannels(ctx *SyncContext, teamID string, teamName string) {
	// GET {BaseURL}/teams/{teamID}/channels?$top=...
	// For each channel -> create folder doc (Team/Channel) and call enumerateMessages(...)
}

func (p *Plugin) enumerateMessages(ctx *SyncContext, teamID, channelID, teamName, channelName string) {
	// GET {BaseURL}/teams/{teamID}/channels/{channelID}/messages?$top=...&$orderby=lastModifiedDateTime desc
	// Filter by ctx.LastKnown, track ctx.LatestSeen, push common.Document for messages
	// Also loop replies endpoint and push documents
}

func max(a, b int) int { if a > b { return a }; return b }
