package microsoft_teams

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"infini.sh/coco/modules/common"
	httprouter "infini.sh/framework/core/api/router"
	"infini.sh/framework/core/kv"
	"infini.sh/framework/core/orm"
	"infini.sh/framework/core/util"
)

// --- watermark helpers (same bucket pattern as Feishu):contentReference[oaicite:16]{index=16}
func (p *Plugin) saveLastModifiedTime(datasourceID, ts string) error {
	bucket := fmt.Sprintf("/connector/%s/lastModifiedTime", p.pluginType)
	return kv.AddValue(bucket, []byte(datasourceID), []byte(ts))
}
func (p *Plugin) getLastModifiedTime(datasourceID string) (string, error) {
	bucket := fmt.Sprintf("/connector/%s/lastModifiedTime", p.pluginType)
	b, err := kv.GetValue(bucket, []byte(datasourceID))
	if err != nil { return "", err }
	return string(b), nil
}

// --- /connector/msteams/connect
func (p *Plugin) connect(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	if p.OAuthConfig == nil || p.OAuthConfig.ClientID == "" || p.OAuthConfig.ClientSecret == "" {
		http.Error(w, "OAuth not configured (client_id/client_secret).", http.StatusServiceUnavailable)
		return
	}
	redirectURL := p.OAuthConfig.RedirectURL
	if !strings.HasPrefix(redirectURL, "http") {
		scheme := "http"
		if req.TLS != nil || req.Header.Get("X-Forwarded-Proto") == "https" { scheme = "https" }
		host := req.Host; if host == "" { host = "localhost:8080" }
		redirectURL = fmt.Sprintf("%s://%s%s", scheme, host, redirectURL)
		p.OAuthConfig.RedirectURL = redirectURL
	}
	// Build authorize URL (using configured AuthURL and Scopes)
	values := url.Values{}
	values.Set("client_id", p.OAuthConfig.ClientID)
	values.Set("response_type", "code")
	values.Set("redirect_uri", redirectURL)
	values.Set("response_mode", "query")
	if len(p.OAuthConfig.Scopes) > 0 {
		values.Set("scope", strings.Join(p.OAuthConfig.Scopes, " "))
	}
	authURL := fmt.Sprintf("%s?%s", p.OAuthConfig.AuthURL, values.Encode())
	http.Redirect(w, req, authURL, http.StatusTemporaryRedirect)
}

// --- /connector/msteams/oauth_redirect
func (p *Plugin) oAuthRedirect(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	code := req.URL.Query().Get("code")
	if code == "" { http.Error(w, "Missing authorization code.", http.StatusBadRequest); return }

	token, err := p.exchangeCodeForToken(code)
	if err != nil { http.Error(w, "Token exchange failed.", http.StatusInternalServerError); return }

	profile, err := p.getUserProfile(token.AccessToken)
	if err != nil { http.Error(w, "Failed to fetch user profile.", http.StatusInternalServerError); return }

	// Create datasource (same fields & naming pattern as Feishu):contentReference[oaicite:17]{index=17}
	ds := common.DataSource{
		SyncConfig: common.SyncConfig{Enabled: true, Interval: "30s"},
		Enabled:    true,
	}
	userID := util.ToString(profile["id"])
	if userID == "" { userID = util.ToString(profile["userPrincipalName"]) }
	if userID == "" { userID = "unknown" }
	ds.ID   = util.MD5digest(fmt.Sprintf("%v,%v", p.pluginType, userID))
	ds.Type = "connector"
	name := util.ToString(profile["displayName"])
	if name == "" { name = "My Microsoft Teams" } else { name = fmt.Sprintf("%s's Microsoft Teams", name) }
	ds.Name = name

	ds.Connector = common.ConnectorConfig{
		ConnectorID: string(p.pluginType),
		Config: map[string]interface{}{
			"access_token":  token.AccessToken,
			"refresh_token": token.RefreshToken,
			"token_expiry":  time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).Format(time.RFC3339),
			"profile":       profile,
		},
	}

	ctx := orm.NewContextWithParent(req.Context())
	if err := orm.Save(ctx, &ds); err != nil {
		http.Error(w, "Failed to save datasource.", http.StatusInternalServerError); return
	}
	http.Redirect(w, req, fmt.Sprintf("/#/data-source/detail/%v", ds.ID), http.StatusTemporaryRedirect)
}
