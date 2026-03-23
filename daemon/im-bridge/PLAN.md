# cmux 元终端 + IM 桥接方案（重构版）

## Context

用户调整了优先级和架构方向：
1. **IM 桥接优先**，先暴露终端，跑通核心流程
2. **Go 实现**，参考 PicoClaw 的 Gateway 架构快速接入多渠道
3. **元终端架构**：cmux 作为任务分发中心，IM ↔ cmux 一一对应映射

## 核心架构：cmux 元终端

```
                    ┌────────────────────────┐
                    │      IM 用户           │
                    │  Telegram/Slack/飞书    │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   Go IM Gateway        │
                    │   (参考 PicoClaw)       │
                    │   Channel Manager      │
                    └───────────┬────────────┘
                                │ Unix Socket
                    ┌───────────▼────────────┐
                    │   元 cmux 实例          │
                    │   (编排中心)            │
                    │                        │
                    │  ┌──────┬──────┬─────┐ │
                    │  │ WS-1 │ WS-2 │ WS-3│ │  ← 每个 IM Agent 对应一个工作区
                    │  │Agent │Agent │Agent│ │
                    │  │Alice │ Bob  │Carol│ │
                    │  └──┬───┴──┬───┴──┬──┘ │
                    └─────┼──────┼──────┼────┘
                          │      │      │
                    Socket│ Socket│ Socket│    ← 可选：操控其他 cmux 实例
                          ▼      ▼      ▼
                    ┌────────┐┌────────┐┌────────┐
                    │cmux-1  ││cmux-2  ││cmux-3  │  ← 工作 cmux（按需）
                    │Claude  ││Codex   ││脚本    │
                    │Code    ││        ││        │
                    └────────┘└────────┘└────────┘
```

## 一一对应映射

```
IM 概念                              cmux 概念
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Agent（一个 AI 助手）            ↔    Workspace（工作区）
  ├── Session 1（独立对话 A）    ↔      ├── Tab 1（终端/浏览器）
  ├── Session 2（独立对话 B）    ↔      ├── Tab 2（终端/浏览器）
  └── Session 3（独立对话 C）    ↔      └── Tab 3（终端/浏览器）

一个 Agent 可以有多个独立对话    ↔    一个工作区可以有多个 Tab
```

| IM 概念 | cmux 概念 | Socket API |
|---------|-----------|------------|
| **Agent**（AI 助手） | **Workspace**（工作区） | `workspace.create` / `workspace.list` |
| **Agent 的多个 Session** | **工作区内的多个 Tab** | Tab 管理（pane/surface） |
| **单个 Session 对话** | **单个 Tab 内的面板** | `surface.create` / `surface.list` |
| **Session 内的消息** | **面板内的终端输入/输出** | `surface.send_text` / `surface.read_text` |
| **新建对话（同 Agent）** | **在工作区内新建 Tab** | `surface.create` / `pane.create` |
| **切换对话（同 Agent）** | **切换 Tab** | `surface.focus` / `pane.focus` |
| **新建 Agent** | **新建 Workspace** | `workspace.create` |
| **切换 Agent** | **切换 Workspace** | `workspace.select` |
| **发送图片/文件** | **浏览器操作** | `browser.open_split` / `browser.screenshot` |
| **通知** | **cmux 通知系统** | `notification.create` |

**示例：**
```
Agent "前端助手"（Workspace-1）
├── Tab 1: Claude Code 修 login bug     ← Session 1（/new fix-login）
├── Tab 2: Claude Code 写测试           ← Session 2（/new write-tests）
└── Tab 3: 浏览器 localhost:3000        ← Session 3（/new preview）

Agent "后端助手"（Workspace-2）
├── Tab 1: Codex 重构 API              ← Session 1
└── Tab 2: 终端跑数据库迁移            ← Session 2
```

## Go IM Gateway 实现

### 参考项目

| 项目 | 价值 | 可复用 |
|------|------|--------|
| **PicoClaw** (sipeed/picoclaw) | Go 多渠道 Gateway 架构，Channel Manager | Gateway + Channel 接口设计 |
| **trsh-go** | Telegram SSH Bot，命令执行 | 终端命令执行 + 输出回传 |
| **shell2telegram** | CLI → Telegram Bot 映射 | 长输出分段、markdown 格式化 |
| **go-lark** | 飞书官方 SDK，650+ 内部用户 | 飞书适配器 |

### 渠道接入（PicoClaw 已支持 17+ 渠道，可直接参考）

| 优先级 | 渠道 | Go SDK | 工期 |
|--------|------|--------|------|
| P0 | Telegram | telebot / telego | 1 周 |
| P1 | Slack | slack-go (Socket Mode) | 1 周 |
| P1 | 飞书 | go-lark | 1 周 |
| P1 | Discord | discordgo | 3 天 |
| P2 | QQ | qq-bot-api / OneBot | 1 周 |
| P2 | 钉钉 | godingtalk | 3 天 |
| P2 | WhatsApp | PicoClaw 参考 | 3 天 |
| P2 | WeCom（企业微信） | PicoClaw 参考 | 3 天 |
| P2 | LINE / Matrix / IRC | PicoClaw 参考 | 各 1-2 天 |

PicoClaw 的 `pkg/channels/` 已实现统一 Channel 接口，可直接复用其适配器代码。

### 文件结构

```
daemon/im-bridge/
├── main.go                         # 入口
├── gateway/
│   ├── server.go                   # HTTP Gateway（参考 PicoClaw）
│   └── router.go                   # 消息路由
├── channels/
│   ├── channel.go                  # Channel 接口定义
│   ├── manager.go                  # Channel Manager（参考 PicoClaw）
│   ├── telegram.go                 # Telegram 适配器
│   ├── slack.go                    # Slack 适配器
│   ├── feishu.go                   # 飞书适配器
│   └── discord.go                  # Discord 适配器
├── bridge/
│   ├── cmux_client.go              # cmux Socket 客户端（JSON-RPC v2）
│   ├── session.go                  # IM 会话 ↔ cmux 工作区映射
│   ├── output_watcher.go           # 终端输出变化检测 + 推送
│   └── formatter.go                # 终端输出 → IM 消息格式化
├── config/
│   └── config.go                   # 配置管理（YAML）
└── go.mod
```

### Channel 接口（参考 PicoClaw）

```go
type Channel interface {
    Name() string
    Start(ctx context.Context) error
    Stop() error
    Send(chatID string, msg OutboundMessage) error
    OnMessage(handler func(msg InboundMessage))
}

type InboundMessage struct {
    ChannelName string
    ChatID      string
    UserID      string
    Text        string
    Attachments []Attachment
}

type OutboundMessage struct {
    Text        string
    Format      string  // "text" | "markdown" | "code"
    Attachments []Attachment
}
```

### cmux Socket 客户端

```go
type CmuxClient struct {
    socketPath string
    conn       net.Conn
    requestID  int
}

func (c *CmuxClient) Call(method string, params map[string]interface{}) (json.RawMessage, error) {
    // JSON-RPC v2 over Unix Socket
    req := map[string]interface{}{
        "jsonrpc": "2.0",
        "method":  method,
        "params":  params,
        "id":      c.requestID,
    }
    c.requestID++
    // encode + send + read response
}

// 关键方法
func (c *CmuxClient) CreateWorkspace(name string) (WorkspaceInfo, error)
func (c *CmuxClient) SendText(surfaceID, text string) error
func (c *CmuxClient) ReadText(surfaceID string) (string, error)
func (c *CmuxClient) ListWorkspaces() ([]WorkspaceInfo, error)
func (c *CmuxClient) Screenshot() ([]byte, error)
```

### 会话管理（核心映射逻辑）

```go
// Agent = Workspace（一个 AI 助手对应一个工作区）
type Agent struct {
    ID          string               // Agent 标识
    Name        string               // Agent 名称（如 "前端助手"）
    WorkspaceID string               // cmux 工作区 ID
    Sessions    map[string]*Session  // sessionID → Session（多个独立对话）
    ActiveSID   string               // 当前活跃的 Session ID
}

// Session = Tab（一个独立对话对应工作区内的一个 Tab）
type Session struct {
    ID          string    // Session 标识
    SurfaceID   string    // cmux Tab 内的终端面板 ID
    Name        string    // Session 名称（如 "fix-login"）
    AgentType   string    // "claude-code" | "codex" | "shell"
    LastOutput  string    // 上次读取的终端输出（用于 diff）
    Watching    bool
}

type SessionManager struct {
    agents map[string]*Agent   // userID → Agent（每个 IM 用户一个 Agent）
    cmux   *CmuxClient
}

func (sm *SessionManager) HandleMessage(msg InboundMessage) {
    agent := sm.GetOrCreateAgent(msg.UserID)

    // 1. 如果是新 Agent，创建 cmux 工作区
    if agent.WorkspaceID == "" {
        ws := sm.cmux.CreateWorkspace("agent-" + msg.UserID)
        agent.WorkspaceID = ws.ID
        // 创建默认 Session（Tab）
        agent.NewSession("default", ws.Surfaces[0].ID)
    }

    // 2. 处理命令
    switch {
    case strings.HasPrefix(msg.Text, "/new "):
        // 在同一 Agent（工作区）内新建 Session（Tab）
        name := strings.TrimPrefix(msg.Text, "/new ")
        surface := sm.cmux.CreateSurface(agent.WorkspaceID)
        agent.NewSession(name, surface.ID)
        sm.Reply(msg, "新对话 ["+name+"] 已创建")

    case msg.Text == "/list":
        // 列出当前 Agent 的所有 Session
        var list []string
        for _, s := range agent.Sessions {
            prefix := "  "
            if s.ID == agent.ActiveSID { prefix = "▶ " }
            list = append(list, prefix + s.Name)
        }
        sm.Reply(msg, "对话列表:\n" + strings.Join(list, "\n"))

    case strings.HasPrefix(msg.Text, "/switch "):
        // 切换到同一 Agent 内的另一个 Session
        name := strings.TrimPrefix(msg.Text, "/switch ")
        agent.SwitchSession(name)
        sm.cmux.FocusSurface(agent.ActiveSession().SurfaceID)

    default:
        // 普通消息 → 发送到当前活跃 Session 的终端
        session := agent.ActiveSession()
        sm.cmux.SendText(session.SurfaceID, msg.Text + "\n")

        // 启动输出监听
        if !session.Watching {
            session.Watching = true
            go sm.WatchOutput(agent, session, msg.ChatID)
        }
    }
}
```

### 输出监听与推送

```go
func (sm *SessionManager) WatchOutput(session *Session) {
    ticker := time.NewTicker(500 * time.Millisecond) // 初始快速轮询
    idleCount := 0

    for range ticker.C {
        output := sm.cmux.ReadText(session.SurfaceID)

        if output != session.LastOutput {
            // 检测到变化
            diff := extractDiff(session.LastOutput, output)
            session.LastOutput = output
            idleCount = 0
            ticker.Reset(500 * time.Millisecond) // 重置为快速

            // 格式化并推送到 IM
            formatted := formatForIM(diff)
            sm.channel.Send(session.ChatID, OutboundMessage{
                Text:   formatted,
                Format: "code",
            })
        } else {
            idleCount++
            if idleCount > 10 { // 5秒无变化
                ticker.Reset(2 * time.Second) // 降为慢速
            }
            if idleCount > 30 { // 60秒无变化
                ticker.Reset(5 * time.Second) // 空闲模式
            }
        }
    }
}
```

## 元终端：cmux 操控其他 cmux

### 已验证的能力

cmux 支持**多实例独立运行**，通过不同 socket 路径隔离：
- 生产：`~/Library/Application Support/cmux/cmux.sock`
- Debug tagged：`/tmp/cmux-debug-{tag}.sock`
- Staging：`/tmp/cmux-staging.sock`

**元终端操控模式：**
```
元 cmux (CMUX_SOCKET=/tmp/cmux.sock)
  → cmux CLI --socket /tmp/cmux-worker-1.sock workspace.create
  → cmux CLI --socket /tmp/cmux-worker-2.sock surface.send_text
```

### 安全隔离

| 实例 | Socket 模式 | 权限 |
|------|-------------|------|
| 元 cmux | `automation` | 允许 IM Gateway 连接 |
| 工作 cmux | `cmuxOnly` | 只允许元 cmux 连接 |

## 实施路线

### Phase 1：IM → 终端直通（3 周）

**目标：** IM 消息直接透传到 cmux 终端，暴露完整终端能力

1. **Go Gateway 骨架**（3 天）
   - 参考 PicoClaw `cmd/picoclaw/internal/gateway/`
   - Channel 接口 + Manager
   - cmux Socket 客户端

2. **Telegram 适配器**（3 天）
   - telebot / telego 集成
   - 消息接收 → `surface.send_text`
   - 终端输出 → Telegram 消息推送

3. **会话管理**（3 天）
   - ChatID ↔ WorkspaceID 映射
   - 自动创建/复用工作区

4. **输出监听**（3 天）
   - `surface.read_text` 轮询 + diff
   - 长输出分段（4096 字符限制）
   - ANSI 颜色剥离 + markdown 代码块格式化

5. **截图能力**（2 天）
   - `/screenshot` 命令 → 终端截图发送到 IM
   - 浏览器截图同理

6. **测试和调优**（2 天）

### Phase 2：多渠道 + 元终端 + 多 Session（4 周）

- Slack / 飞书 / Discord 适配器（参考 PicoClaw 17+ 渠道）
- 元终端多工作区编排（多 Agent 并行）
- 同一 Agent 内的多 Session 管理（多 Tab）
- IM 命令系统：

| 命令 | 作用 | cmux 操作 |
|------|------|-----------|
| `/new <name>` | 在当前 Agent 内新建 Session | `surface.create`（新 Tab） |
| `/list` | 列出当前 Agent 的所有 Session | `surface.list` |
| `/switch <name>` | 切换到另一个 Session | `surface.focus` |
| `/close` | 关闭当前 Session | `surface.close` |
| `/agent new <name>` | 新建 Agent | `workspace.create`（新工作区） |
| `/agent list` | 列出所有 Agent | `workspace.list` |
| `/agent switch <name>` | 切换 Agent | `workspace.select` |
| `/screenshot` | 截图当前 Session | `browser.screenshot` / 终端截图 |
| `/split` | 在当前 Session 内分屏 | `surface.split` |

### Phase 3：ChatPanel + AI（4-6 周）

- Swift 原生 ChatPanel（内嵌聊天面板）
- Claude API 集成（AgentLoop + ToolRegistry）
- 分层透明度模型（L1-L4）

## 验证方法

1. 启动 cmux，确认 socket 可访问（`automation` 模式）
2. 启动 `daemon/im-bridge`：`go run . --config config.yaml`
3. Telegram 发送 `echo hello` → cmux 终端执行 → 输出推送回 Telegram
4. 发送 `ls -la` → 格式化为代码块推送
5. 发送 `/screenshot` → 终端截图发送到 Telegram
6. 发送 `/new` → 创建新工作区 → 切换到新会话
7. 多用户同时操作不同工作区，验证隔离性

## 关键参考文件

| 文件 | 用途 |
|------|------|
| `Sources/TerminalController.swift` | Socket 命令处理（v2 JSON-RPC） |
| `Sources/SocketControlSettings.swift` | Socket 路径/权限/多实例 |
| `CLI/cmux.swift` | CLI socket 连接逻辑 |
| `Sources/Workspace.swift` | 工作区模型 |
| `daemon/remote/` | Go JSON-RPC daemon 参考 |
| `tests_v2/cmux.py` | Python socket 客户端参考 |
