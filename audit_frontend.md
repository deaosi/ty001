# 探域数据看板 index.html 前端审查报告

- 审查对象：`D:\test\test01\static\app\index.html`（约 3179 行，单文件包含 dashboard/shops/staff/trace/tasks/logs/settings/more 八个页面模块的 HTML+CSS+JS，原生 JS + Tabler v1.3，无构建工具）
- 审查结论：A 类 12 项（BUG/隐患）、B 类 8 项（性能与体验）、C 类 6 项（功能完整性）、D 类 7 项（小优化）
- 修复优先级建议：A1 → A2/A3 → A4 → A5 → A6

---

## A. BUG / 隐患（最高优先）

### A1. XSS 漏洞（属性实体二次解码使 esc() 失效）— 最严重

- **位置**：`renderGroupStatus` L1497、`renderShopsTable` L1815/L1823、`shopDetail` L1893/L1950、`wxQrPoll` L2702、`renderGroups` L2754、`loadRiskForBell` L3031
- **问题**：所有 `onclick="app.xxx('" + esc(data) + "')"` 都假设 `esc()` 能防注入，但 HTML 属性解析器会把 `&#39;` 解码回 `'`，使恶意数据逃逸出 JS 字符串。例如店铺名含 `');alert(document.cookie)//` 时，最终执行的是 `app.shopDetail('x');alert(document.cookie)//')`。店铺名/集团名/group_id 来自探域外部平台数据，属可控输入。
- **修复**：不要往 JS 字符串内联用户数据——改用 `data-id` 属性 + 事件委托；最小改动是 `onclick="app.xxx(' + esc(JSON.stringify(v)) + ')"`（JSON 双引号经 `&quot;` 往返后仍是合法 JS 字符串，单引号在双引号串内安全）。

### A2. 请求竞态：快速切换时旧响应覆盖新数据

- **位置**：`renderShops` L1756-1793、`renderStaff` L2028-2069、`renderTrace` L2252-2284、`renderTasks` L2455-2489、`loadMsgData` L2338-2348、`shopDetail` L1873-1952
- **问题**：这些函数无请求序号/AbortController 保护，都是"清空→fetch→写 DOM"。快速切平台/店铺/日期时，后发先至的旧响应会覆盖新数据（如 L1766 平台快速切换、L2342 连续切日期）。
- **修复**：仿照已有 `chartRun` 守卫（L1347-1350），每个渲染函数加 `var run = ++xxxRun; ... if (run !== xxxRun) return;`；或在 `api()` L1094 统一用 AbortController 取消过期请求。

### A3. dashboard 首屏双重渲染（重复请求翻倍）

- **位置**：`route()` L1161-1162 + `renderPage` map L1168
- **问题**：首次进入 dashboard 时，L1161 调 `renderPage('dashboard')` → `renderDashboard()`，紧接着 L1162 `if (name === 'dashboard') renderDashboard()` 又整体渲染一次。`/api/overview`、`/api/db/status`、`/api/tasks/list`、`/api/logs` 及 3 个平台核算全部请求 ×2。
- **修复**：从 `renderPage` 的 map 中移除 `dashboard`（L1162 已覆盖其重渲染语义），或 L1161 先渲染、L1162 仅当 `rendered.dashboard` 已存在时执行。

### A4. 定时器不清理 / 页面隐藏与退出后仍空转

- **位置**：`boot()` L3140-3146（30s 轮询 loadRisk/loadRiskForBell + 5s 轮询 renderTasks）、`logout` L3058-3064、`wxQrStart/wxQrPoll` L2663-2725
- **问题**：① 两个 `setInterval` 无清理，退出登录后仍每 30s/5s 打 401 请求；② 无 `document.visibilitychange` 暂停，标签页后台时 5s 轮询仍全量重渲染任务表（每次还带一个统计请求）；③ `wxQrTimer`（L2683）离开设置页后仍每 2s 轮询 `/api/wx-qr/status` 直到过期，且 `wxQrPoll` 无 in-flight 防重入。
- **修复**：将 interval id 存变量，`logout`/`pagehide` 时 `clearInterval`；轮询回调首行判断 `document.hidden` 直接 return；`wxQrPoll` 加 `if (busy) return` 标志。

### A5. 切换集团/同步店铺后 allShops 缓存不失效（陈旧数据）

- **位置**：`switchGroup` L1215-1227、`loadTraceShops` L2220-2231、`syncShops` L3003-3006、`boot` L3135
- **问题**：`allShops` 只在启动时清空（L3135）；切集团后 `loadTraceShops` 因 `allShops.length` 非空直接跳过拉取，消息轨迹店铺下拉仍显示旧集团的店铺（`traceShopPlatform` 又恰好等于新平台时会整个跳过重建）。同时 `state.selShops` 未清空，旧集团勾选会随"核算所选"发出。
- **修复**：`switchGroup`/`syncShops` 成功后置 `allShops = []`、`traceShopPlatform = null`、`state.selShops = {}`。

### A6. 分页越界：点"上一页"回到第 0 页

- **位置**：`shopsPage` L1870 + `renderShopsTable` L1802-1807；`tasksPage` L2490 + `renderTasks` L2459-2460
- **问题**：翻页只做上界钳制（L1803 `Math.min`、L2460），无下界。第 1 页点"上一页"→ page=0 → `slice(-8, 0)` 返回空 → 表格显示"没有符合条件的店铺"，页脚显示"第 0 / N 页"。
- **修复**：`state.shopsPage = Math.max(1, Math.min(pages, state.shopsPage + d))`（pages 在 renderShopsTable 内已算出，可把钳制逻辑挪进渲染函数）。

### A7. api() 无超时，挂起请求永久"加载中"

- **位置**：`api()` L1094-1107
- **问题**：fetch 无 AbortController 超时；后端假死时所有调用方永久停在加载态，无任何提示。
- **修复**：`api()` 内 `AbortController` + 15~30s 超时，超时抛可识别错误；建议同时加 `cache: 'no-store'` 防浏览器缓存陈旧 GET。

### A8. checkAuth 把瞬时网络错误当成未登录踢回 /login

- **位置**：`checkAuth` L1108-1120
- **问题**：`catch` 里只判断 `authRedirecting`，`/api/auth/me` 因网络/500 失败也会落入 `goLogin()`，弱网时用户被反复踢出。
- **修复**：捕获时区分 401（走 goLogin）与其他错误（保留登录态并提示/重试）。

### A9. showModal 重复打开会累积 backdrop

- **位置**：`showModal/hideModal` L1031-1049
- **问题**：同一 modal 已显示时再次 showModal（如快速连点两次"明细"）会再 append 一个 backdrop；hideModal 只 remove 第一个，残留的 backdrop 遮罩和 `modal-open` 状态损坏页面。
- **修复**：showModal 首行 `if (el.classList.contains('show')) return;`；hideModal 用 `querySelectorAll` 全量清除。

### A10. paintIcons 重置通知红点状态

- **位置**：`paintIcons` L3097 vs `loadRiskForBell` L3023-3024
- **问题**：每次 paintIcons（切主题 L3055、进设置页 L2920）都重建 `btnBell.innerHTML` 并把红点设回 `display:none`，未读角标在下次 30s 轮询前消失。
- **修复**：paintIcons 只更新图标，红点显示逻辑集中到 loadRiskForBell（或在重建后恢复 `dot.style.display`）。

### A11. 二维码 img src 未转义

- **位置**：`wxQrStart` L2679
- **问题**：`d.qrImage` 直接拼进 `innerHTML`，若后端返回异常内容（含 `"` 引号）可注入属性。`d.qrCodeUrl` 走了 esc 而 `qrImage` 没有，逻辑不一致。
- **修复**：`esc(d.qrImage || d.qrCodeUrl)`。

### A12. 时间解析两处隐患

- **位置**：`fmtTime` L1064-1069、`normalizeDate` L2287-2291
- **问题**：① `fmtTime` 对**数字字符串**时间戳（如 `"1712345678"`）走 `new Date(str)` → Invalid Date → 原样显示 epoch 数字；② `normalizeDate` 对 `MM-DD` 格式补**当前年份**，跨年（如 12 月数据 1 月查看）消息明细会按错年份请求。
- **修复**：`+ts` 强转统一数字/字符串 epoch；normalizeDate 依据 `msgState.days` 里同店最近日期推断年份。

---

## B. 性能与体验

### B1. dashboard 每次进入全量重拉 ~8-10 个请求

- **位置**：`renderDashboard` L1255-1308（overview + 3×平台核算 L1312-1323 + 趋势 L1429 + db L1506 + 任务 L1529 + 日志 L1545）；`toggleTheme` L3056 也整页重渲染
- **建议**：按 key+参数做 60s TTL 内存缓存（`lastFetch[key]`）；切主题只重建图表（destroy/recreate），不要重拉数据。

### B2. 输入框无防抖，每键全量重渲染

- **位置**：`msgKw` L905 → `renderMsgList` L2350（每条 keystroke 对全量消息数组 filter + 重建 innerHTML）、`shopsSearch` L462 → `renderShopsTable` L1794、`staffSearch` L545 → `renderStaffGrid` L2132
- **建议**：三处加 250ms 防抖（`oninput` 里 `clearTimeout` + `setTimeout`）。

### B3. 系统日志标签筛选触发整页重拉

- **位置**：`renderSysLogs` L2588 `onchange="app.renderSysLogs()"` → L3163 暴露为 `function () { renderLogs(); }` → 重新请求 `/api/logs/center`
- **建议**：缓存最近一次 `{logs, tags}` 响应，标签切换只做本地 filter；L3163 改为直接调用 `renderSysLogs(缓存数据)`。

### B4. loadPlatformTotals 硬编码平台 [1,5,7]

- **位置**：L1312-1323
- **问题**：忽略 `state.platform`，当前集团是天猫导入平台（10/11）时"消息量按平台/占比"图表仍展示拼多多/抖音/京东，数据误导；且每次 dashboard 渲染固定 3 个并行请求。
- **建议**：按 `state.platform` 决定平台列表（或后端支持时一次返回全部），并按 `days` 值缓存结果。

### B5. 任务页 5s 轮询附带独立统计请求

- **位置**：L3144-3146 + `renderTaskStats` L2437-2454
- **建议**：统计卡与列表用同一次 `/api/tasks/list?limit=100` 响应前端分页/计数，或降低轮询频率至 10s，并配合 `document.hidden` 暂停。

### B6. Chart.js 走 CDN 无 SRI

- **位置**：L959
- **建议**：与 tabler 一样下沉到 `/static/vendor`（内网/断网环境图表不失效），至少加 `integrity` SRI。

### B7. 缺骨架屏

- **位置**：`tasksBody` L638、`taskStatCards` L630、日志三张表 L678/684/690、`groupsBody` L760 在数据返回前是空白
- **建议**：统一加"加载中…"占位行（shops/trace 已做，风格对齐即可）。

### B8. renderActivity 空日志时串行二次请求

- **位置**：L1554-1555
- **建议**：与 `renderRecentTasks` 的 `/api/tasks/list` 合并成一次请求，或并行发起。

---

## C. 功能完整性

### C1. 日志中心无分页/加载更多，早期记录不可达

- **位置**：L2547 `op_limit=50&task_limit=20&sys_limit=50`
- **建议**：加 offset/limit 分页参数或"加载更多"按钮；同时补"导出当前 Tab"。

### C2. notifyClearAll 是死代码

- **位置**：`notifyClearAll` L3046-3050 已实现并暴露（L3166），但 HTML 通知面板（L240）无任何按钮引用它
- **建议**：通知下拉加"全部已读/清空"按钮并绑定。

### C3. apiStateBadge 从不更新 + 大量静默 catch

- **位置**：`apiStateBadge` L263（全文件仅此一处，无 JS 引用）；静默吞错：`loadRisk` L3015、`loadRiskForBell` L3038、`loadTraceShops` L2228、`loadMsgDays` L2335、`fillWeekSelect` L1742、`renderTaskStats` L2453、`loadCookieExpiry` L2627、`notifyRead` L3043
- **问题**：后端异常时用户零感知（店铺下拉空、周选择器空、通知不更新），顶栏还常驻"服务正常"。
- **建议**：统一 `fetchError(msg)` 助手（toast + 状态徽标变红），至少把 `loadTraceShops`/`fillWeekSelect`/`loadMsgDays` 的失败提示到用户。

### C4. 全局搜索未实现

- **位置**：L261 搜索框 → `searchHint` L3078 仅 toast
- **建议**：要么移除入口，要么实现跨模块过滤（可先只跳转对应 tab 并预填搜索词）。

### C5. 导出与当前视图不一致 + 消息轨迹无导出

- **位置**：`exportShops` L2019-2025、`exportStaff` L2210-2216（导出全量缓存，忽略搜索/排序/分页）；trace 页只有日期汇总，无原始消息导出
- **建议**：导出前应用当前 filter/sort；trace 加"导出该区间明细 CSV"。

### C6. sysTagSel 在 op/task tab 下仍显示

- **位置**：L673 `logFilterBox` + L2588
- **建议**：`switchLogs` L2598-2606 里按 tab 显隐筛选框。

---

## D. 小优化（一句话）

- D1：`iso()` 在 L1637/L1675/L1689 三处重复定义 → 提取全局。
- D2：`pct()` L1012 的 `Number(n) == null` 恒为 false（死代码）；`msgState.loaded` L2286/L2339/L2344 只写不读。
- D3：`paintIcons` 在 `renderDingtalk` L2786、`renderSettings` L2920 冗余调用。
- D4：`refresh()` L3069 的旋转动画缺 CSS `transition`（.icon-btn 无过渡，旋转不可见）。
- D5：`fillPlatformSelect` L1180-1181 fetch/import 平台可能重复，未按 id 去重。
- D6：内联 `onclick` 与 `addEventListener` 混用，建议统一为事件委托（顺带根治 A1）。
- D7：无 i18n，文案硬编码在 HTML/JS——单语言产品可接受，但可抽常量便于维护。

---

## 总结（最重要的 5 个问题）

第一是 onclick 属性拼接的 XSS/破损（A1）：`esc()` 在 HTML 属性→JS 字符串双重解码下失效，含单引号的店铺名/集团名会直接打破按钮甚至执行脚本，需全面改用 data 属性委托或 `esc(JSON.stringify())`；第二是请求竞态与 dashboard 首屏双渲染（A2/A3），快速切换平台或首次加载都会出现旧数据覆盖新数据、请求量翻倍，补齐序号守卫并移除 map 中的 dashboard 即可；第三是定时器治理缺失（A4），30s/5s/2s 三组轮询在页面隐藏、退出登录后仍空转，应统一 visibilitychange 暂停 + logout 清理；第四是切集团/同步店铺后 `allShops`、`traceShopPlatform`、`selShops` 缓存不失效（A5），消息轨迹会展示旧集团店铺、核算所选会发出旧勾选；第五是分页下界越界与全静默错误（A6/A7/C3），上一页可翻到第 0 页显示空表，而大量 `catch {}` 让用户对后端故障零感知且"服务正常"徽标恒绿。建议按 A1→A2/A3→A4→A5 的顺序修复，其余条目作为优化 backlog。
