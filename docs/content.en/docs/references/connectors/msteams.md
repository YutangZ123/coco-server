---
title: "Microsoft Teams"
weight: 36
description: "Index and sync Microsoft Teams messages, channels, and files via Microsoft Graph."
---

# Microsoft Teams Connector

The Microsoft Teams connector indexes collaboration content from Microsoft Teams via Microsoft Graph, including **teams, channels, messages, replies, and files** (SharePoint/OneDrive-backed).

## Features

- 🔍 **Unified Search**: Keyword-based search across Teams messages, channels, and files  
- 🧭 **Hierarchy Awareness**: Team → Channel → (Folders) path preserved in indexed docs  
- 🔐 **OAuth 2.0**: Standard Microsoft identity platform OAuth with refresh token handling  
- ⚡ **Efficient Sync**: Scheduled and manual synchronization  
- 🔄 **Incremental Updates**: Last-modified watermark with safety buffer  
- 🗂️ **Recursive Files**: Walk channel “Files” (SharePoint/OneDrive) recursively  
- 📦 **Queue-First**: Push lightweight documents; downstream extractors can enrich content  
- 🌐 **Multi-Cloud Ready**: Configurable endpoints for Global / US Gov / China national clouds  

---

## Supported Surface

- **Teams** (M365 Groups)  
- **Channels** (standard & private\*)  
  \*Private channels require appropriate app permissions/tenant policy  
- **Channel Messages** & **Replies**  
- **Files** in the channel document library (SharePoint/OneDrive)  

---

## Authentication

### OAuth 2.0 (Recommended)

Uses Microsoft identity platform OAuth2 to obtain `access_token` and `refresh_token`.

#### Requirements

- `client_id` — Azure AD application (App registration)  
- `client_secret` — Client credential  
- `redirect_url` — Your backend: `/connector/msteams/oauth_redirect`  
- `auth_url` — e.g. `https://login.microsoftonline.com/common/oauth2/v2.0/authorize`  
- `token_url` — e.g. `https://login.microsoftonline.com/common/oauth2/v2.0/token`  
- `scope` — Space-delimited Graph scopes (see below)  

#### Flow

1. Configure connector OAuth (client id/secret, URLs, scopes)  
2. Click **Connect** in the product UI  
3. Complete Microsoft login/consent  
4. Connector stores `access_token`, `refresh_token`, and expiry in the datasource  
5. Scheduled syncs automatically refresh tokens  

---

### Service Token (Advanced)

If you configure a **tenant-wide app-only token** (Client Credentials) or a pre-provisioned user token, you can place it in `user_access_token`.  
This bypasses per-user OAuth, but you must manage lifetimes and permissions manually.

> For parity with other connectors, OAuth is preferred for usability and security.

---

## Required Microsoft Graph Permissions (Scopes)

> Choose the least-privileged scopes that meet your needs.

**Common read scopes for indexing:**

- `User.Read` — basic profile (used during OAuth to label datasource)  
- `Group.Read.All` — enumerate joined teams (M365 groups)  
- `Channel.ReadBasic.All` — list channels in teams  
- `ChannelMessage.Read.All` — read channel messages  
- `Files.Read.All` or `Files.ReadWrite.All` — read files via SharePoint/OneDrive  
- `offline_access` — refresh token support  

> If you need to index private channels or tenant-wide content, app permissions (application scopes) may be required and tenant admin consent must be granted.

---

## Configuration Architecture

### Connector Level (OAuth configuration)

Manages OAuth and defaults centrally; datasources are created via OAuth.

```yaml
connector:
  msteams:
    enabled: true
    interval: "30s"
    page_size: 100
    config:
      # OAuth
      auth_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
      token_url: "https://login.microsoftonline.com/common/oauth2/v2.0/token"
      redirect_url: "/connector/msteams/oauth_redirect"
      client_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      client_secret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      scope: "User.Read Group.Read.All Channel.ReadBasic.All ChannelMessage.Read.All Files.Read.All offline_access"

      # Graph base (auto-switch by region if you support it)
      graph_url: "https://graph.microsoft.com/v1.0"   # Global default
      # graph_url: "https://graph.microsoft.us/v1.0"  # US Gov
      # graph_url: "https://microsoftgraph.chinacloudapi.cn/v1.0"  # China
