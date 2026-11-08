# 模块精读 04 · frontend

> React 18 + Ant Design 5 + ProComponents 的管理界面。四个页面：系统概览、规则管理、风险记录、数据看板。
> 这一层没有复杂算法，**重点在"前后端字段契约"和"请求层的两种写法"**——联调 bug 几乎都出在这里。
>
> 返回：[学习总览](../LEARNING-GUIDE.md) · 上一篇：[03 · web](03-flink-risk-web.md)

---

## 目录

1. [工程与命令](#1-工程与命令)
2. [构建配置](#2-构建配置)
3. [启动链路：从 index.html 到页面](#3-启动链路从-indexhtml-到页面)
4. [布局与路由](#4-布局与路由)
5. [请求层（重点）](#5-请求层重点)
6. [类型与常量](#6-类型与常量)
7. [页面精读](#7-页面精读)
8. [样式体系](#8-样式体系)
9. [与后端的字段契约](#9-与后端的字段契约)
10. [已知问题](#10-已知问题)
11. [读完后应该能回答的问题](#11-读完后应该能回答的问题)

---

## 1. 工程与命令

```json
"scripts": {
  "dev": "vite",
  "build": "tsc && vite build",
  "preview": "vite preview",
  "lint": "eslint src --ext .ts,.tsx"
}
```

```bash
cd frontend
npm install
npm run dev      # http://localhost:3000
npm run build    # 先 tsc 类型检查，再 vite 打包到 dist/
npm run lint
```

关键依赖：

| 包 | 用途 |
|----|------|
| `react` / `react-dom` 18.3 | 框架 |
| `react-router-dom` 6 | 路由 |
| `antd` 5.22 + `@ant-design/icons` | 基础组件 |
| `@ant-design/pro-components` 2.8 | `ProTable`（带搜索的表格）、`ModalForm`（弹窗表单） |
| `antd-style` 3.6 | CSS-in-JS（`createStyles`） |
| `axios` | HTTP |
| `dayjs` | 日期（**当前代码未使用**，是预留） |

`build` 里先跑 `tsc`：**类型错误会让打包失败**。而 `tsconfig.json` 开了 `strict: true`、但 `noUnusedLocals/noUnusedParameters` 为 `false`（所以存在未使用变量也不会报错，比如 `dayjs`、`services/` 目录）。

---

## 2. 构建配置

```ts
export default defineConfig({
  plugins: [react()],
  resolve: { alias: { '@': path.resolve(__dirname, 'src') } },
  server: {
    port: 3000,
    proxy: { '/api': { target: 'http://localhost:8080', changeOrigin: true } },
  },
  build: { outDir: 'dist', sourcemap: true },
});
```

三件事：

1. **`@` 别名**：`import api from '@/utils/request'`。`tsconfig.json` 的 `paths` 与 `vite.config.ts` 的 `resolve.alias` **必须同时配置**，只配一个会出现"编辑器认识、打包报错"或反之。
2. **`/api` 代理**：开发时前端 3000 直接请求 `/api/xxx`，Vite 转发到 8080，浏览器同源、无跨域。后端另外还开了 CORS（`WebConfig`），属于双保险。
3. **`sourcemap: true`**：生产构建也带 sourcemap，方便排查但会暴露源码，按需关掉。

`index.html` 只有 `#root` 和 `<script type="module" src="/src/main.tsx">`，是标准 Vite 入口。

---

## 3. 启动链路：从 index.html 到页面

```
index.html
  └─ /src/main.tsx
       └─ <React.StrictMode>
            └─ <ConfigProvider locale=zhCN theme={tokens + components}>
                 └─ <App />                     App.tsx
                      └─ <BrowserRouter>
                           └─ <Routes>
                                └─ <Route path="/" element={<AppLayout/>}>
                                     ├─ index        → pages/Home
                                     ├─ path="rules" → pages/Rule/List
                                     ├─ path="risks" → pages/Risk
                                     └─ path="dashboard" → pages/Dashboard
```

`main.tsx` 做的是**主题注入**，不是业务逻辑：

```tsx
<ConfigProvider
  locale={zhCN}
  theme={{
    algorithm: theme.defaultAlgorithm,
    token: {
      colorPrimary: '#2563eb',
      borderRadius: 12,
      fontFamily: "'Outfit', -apple-system, ...",
      controlHeight: 36,
    },
    components: { Layout: {...}, Menu: {...}, Card: {...}, Table: {...}, Button: {...} },
  }}
>
```

设计要点：**主题从 token 层统一改**，页面里不写死颜色。`index.css` 负责全局重置与字体引入，`App.css` 负责布局类（`.app-layout` / `.app-sider` / `.app-logo` / `.app-content`）。

`React.StrictMode` 会让开发环境下的 `useEffect` 执行两次——所以控制台看到两次 `/api/stats/overview` 请求是正常的，不是 bug。

---

## 4. 布局与路由

`App.tsx` 手写了一个 `Layout`，**没有用 ProLayout**（`docs/ARCHITECTURE.md` 说的 ProLayout 已过时）：

```tsx
const menuItems = [
  { key: '/',          icon: <DashboardOutlined />,          label: '系统概览' },
  { key: '/rules',     icon: <SafetyCertificateOutlined />,  label: '规则管理' },
  { key: '/risks',     icon: <AlertOutlined />,              label: '风险记录' },
  { key: '/dashboard', icon: <BarChartOutlined />,           label: '数据看板' },
];
```

菜单高亮逻辑：

```tsx
const selectedKey = menuItems.find((item) =>
  item.key === '/' ? location.pathname === '/' : location.pathname.startsWith(item.key),
)?.key || '/';
```

`/` 必须单独判断（否则 `startsWith('/')` 永远命中首页），其余用 `startsWith` 以支持未来的子路由。

`AppLayout` 里 `<Outlet />` 是子路由出口；`Sider` 的折叠由本地 `useState` 控制（`trigger={null}` 关掉了自带的底部触发器，靠 CSS 里的 `.ant-layout-sider-trigger` 定制样式）。

---

## 5. 请求层（重点）

### 5.1 两个并存的东西

这个仓库里存在**两套请求封装**，必须先认清哪套在用：

| 位置 | 形态 | 是否被页面使用 |
|------|------|----------------|
| `src/utils/request.ts` | `axios.create` 实例 + 两个 hook | ✅ **页面全部用它** |
| `src/services/rule.ts`、`risk.ts`、`stats.ts` | 每个接口一个导出函数 | ❌ **无人引用**（`grep '@/services'` 为空） |

所以：**改接口路径要改 `utils/request.ts` 的调用方（页面），改 `services/` 不会生效。** `services/` 属于早期的 Api 层残留，可作为"接口清单"参考，但不要以为它在工作。

### 5.2 `api` 实例与响应拦截器

```ts
const api = axios.create({ baseURL: '/api', timeout: 15000 });

api.interceptors.response.use(
  (response) => response.data,          // ← 关键：直接返回 body
  (error) => {
    const msg = error.response?.data?.message || error.message || '请求失败';
    console.error('[API Error]', msg);
    return Promise.reject(error);
  },
);
```

拦截器把 `AxiosResponse<T>` 解包成 `T`，所以运行时 `await api.get('/rules')` 得到的就是 `{data,total,page,pageSize}`。

**但 TypeScript 的类型声明没有变**：`axios.get<T>` 的返回类型仍是 `Promise<AxiosResponse<T>>`。于是页面里出现了这种写法：

```ts
const res = await api.get<PageResult<Rule>>('/rules', { params: {...} });
return { data: (res as unknown as PageResult<Rule>).data || [], ... };
```

`as unknown as T` 是在**强行修正拦截器造成的类型偏差**。更干净的做法是给 `api` 声明一个泛型包装类型，或改用 `request<T>` 函数。

### 5.3 `useRequest`：通用请求 hook

```ts
export function useRequest<T>(requestFn: () => Promise<T>, options?: {...}) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const mountedRef = useRef(true);
  const requestFnRef = useRef(requestFn);
  requestFnRef.current = requestFn;      // 避免函数引用变化触发重复请求
  ...
  const run = useCallback(async () => { ... }, []);   // 引用稳定

  useEffect(() => {
    mountedRef.current = true;
    if (options?.immediate !== false) run();
    return () => { mountedRef.current = false; };
  }, [run, options?.immediate]);

  return { data, loading, error, run, setData };
}
```

三个值得学的细节：

1. **`requestFnRef`**：如果直接把 `requestFn` 放进 `useEffect` 依赖，父组件每次渲染都会生成新函数 → 无限请求。用 ref 存最新函数、依赖数组留空，是这个问题的标准解法。
2. **`mountedRef`**：组件卸载后 `setState` 会警告，这里在 cleanup 里置 false 并据此跳过。
3. **`immediate`**：默认挂载即请求；传 `false` 则手动 `run()`。

### 5.4 `usePageRequest`：已实现但未使用

`utils/request.ts` 底部还导出一个分页 hook（`fetch(page,pageSize,extra)` + `params` 合并），但**没有任何页面使用**——规则页和风险页都直接用 `ProTable` 的 `request` 回调。读代码时不必纠结它的行为。

---

## 6. 类型与常量

### 6.1 `types/index.ts`

```ts
export interface Rule {
  id?: number;
  ruleName: string;         // ← 对应后端 Rule.ruleName（影子字段）
  ruleType?: string;        // ← 后端不存，永远 undefined
  riskLevel: string;
  action?: string;
  ruleExpression: string;   // ← 对应后端 Rule.ruleExpression
  description?: string;
  enabled?: boolean;
  createTime?: string;
  updateTime?: string;
}

export interface RiskRecord { id, userId, eventId, eventType, riskLevel, riskScore, action, details, createTime }
export interface PageResult<T> { data: T[]; total: number; page: number; pageSize: number }
export interface OverviewStats { totalRules, enabledRules, todayRisks, todayHighRisks, ruleTypeDistribution, riskLevelDistribution, eventTypeDistribution }
export interface TrendData { dailyData: Array<{ date; total; high }> }
export interface TopRuleData { topRules: Array<{ ruleName; hitCount }> }
```

注意 `Rule` 用的是**别名**（`ruleName`/`ruleExpression`），不是数据库列名（`name`/`expression`）。后端的 `syncToFrontend()` 负责补齐这两个字段，`syncFromFrontend()` 负责反向拷贝——前端只要按别名收发即可。

`services/stats.ts` 里又定义了一遍 `OverviewStats`/`TrendData`/`TopRuleData`，与 `types/index.ts` 重复；因为 `services` 无人使用，暂无冲突，但属于该清理的重复。

### 6.2 `constants/index.ts`：三张配置表

```ts
export enum RiskLevel { LOW, MEDIUM, HIGH, CRITICAL }
export enum RiskAction { ALLOW, VERIFY, REVIEW, LIMIT, BLOCK, DISABLE }
export enum EventType { LOGIN, ORDER, ACTIVITY }

export const RISK_LEVEL_CONFIG = {
  LOW:      { label: '低',   color: '#52c41a', status: 'Success' },
  MEDIUM:   { label: '中',   color: '#faad14', status: 'Warning' },
  HIGH:     { label: '高',   color: '#ff4d4f', status: 'Error'   },
  CRITICAL: { label: '严重', color: '#cf1322', status: 'Error'   },
};
// ACTION_CONFIG、EVENT_TYPE_CONFIG、RULE_TYPE_CONFIG 同构
```

两个用途：

1. 作为 `ProTable` 的 `valueEnum`（下拉筛选）——需要 `status` 字段（antd 的 Badge 状态）；
2. 作为 `StatusTag` 的渲染配置——只需要 `label` + `color`。

`StatusTag` 把三种类型收进一张 map：

```tsx
const configMap = { riskLevel: RISK_LEVEL_CONFIG, action: ACTION_CONFIG, eventType: EVENT_TYPE_CONFIG };
const config = configMap[type]?.[value];
return config ? <Tag color={config.color}>{config.label}</Tag> : <Tag>{value}</Tag>;
```

未知值降级为显示原始字符串，不会崩——这是前端对"后端新增枚举值"的正确防御。

**新增一个 `action` 取值（比如 `FREEZE`）时**，要同时改 `RiskAction` 枚举与 `ACTION_CONFIG`，否则列表里显示原始英文、筛选项缺失。

---

## 7. 页面精读

### 7.1 Home（系统概览）

```tsx
const { data: stats, loading } = useOverview();       // → /api/stats/overview

<Row gutter={[16, 16]}>
  <Col xs={24} sm={12} lg={6}>
    <StatCard title="规则总数" value={stats?.totalRules ?? 0} icon={<SettingOutlined />} onClick={() => navigate('/rules')} />
  </Col>
  ...
</Row>
```

结构分三块：**总体统计（4 卡）→ 规则维度（按 `RULE_TYPE_CONFIG` 循环）→ 风险维度（按 `RISK_LEVEL_CONFIG` 循环）**。

两个细节：

- `stats?.totalRules ?? 0`：数据未回来时显示 0 而不是空白，避免布局跳动；
- 卡片点击跳转对应列表页，是一种低成本的"下钻"。
- 循环渲染 `Object.entries(RULE_TYPE_CONFIG)` 意味着**后端返回的分布 key 必须与前端常量一致**（`LOGIN/ORDER/ACTIVITY/OTHER`）。后端 `inferRuleType` 猜错类型，前端就显示 0。

`StatCard`：

```tsx
<Card className={onClick ? styles.clickableCard : styles.card} loading={loading} bordered={false} onClick={onClick}>
  <Statistic title={title} value={value} valueStyle={valueStyle} />
  {icon && <div className={styles.icon}>{icon}</div>}
</Card>
```

### 7.2 Rule/List（规则管理，最复杂的一个文件）

#### 7.2.1 `ProTable` 与请求映射

```tsx
<ProTable<Rule>
  actionRef={actionRef}
  columns={columns}
  rowKey="id"
  pagination={{ pageSize: 10 }}
  search={{ labelWidth: 'auto' }}
  request={async (params) => {
    const res = await api.get<PageResult<Rule>>('/rules', {
      params: {
        page: params.current,
        pageSize: params.pageSize,
        name: params.ruleName,          // ← 前端 ruleName → 后端 name
        riskLevel: params.riskLevel,
      },
    });
    return { data: res.data || [], success: true, total: res.total || 0 };
  }}
/>
```

`ProTable` 的约定：`request` 必须返回 `{ data, success, total }`，`params` 里带 `current`/`pageSize` 和所有可搜索列的值。**`params.ruleName` 来自列的 `dataIndex`**——这就是为什么后端参数要映射成 `name`。

**一个真实的 bug**：`action` 列没有 `search: false`，所以会出现"执行动作"筛选框；但 `request` 只传了 `name` 和 `riskLevel`，**用户选了动作也不会过滤**（后端其实支持 `action` 参数）。修法：把 `action: params.action` 加进 params。

同理，`enabled` 列写了 `search: false`，所以没有状态筛选。

#### 7.2.2 列定义与内联操作

```tsx
{
  title: '状态', dataIndex: 'enabled', width: 80, search: false,
  render: (_, record) => (
    <Switch checked={record.enabled} checkedChildren="启用" unCheckedChildren="停用"
      onChange={async (checked) => {
        await api.put(`/rules/${record.id}/toggle`);
        message.success(checked ? '已启用' : '已停用');
        actionRef.current?.reload();
      }} />
  ),
},
```

注意：调的是 `/toggle`（后端取反），却把 `checked` 只用于提示文案。**如果请求失败，开关的视觉状态会与数据库不一致**（没有回滚、没有 catch）。更稳的写法是用 `switchLoading` + 失败时 `reload()`。

列 `riskLevel` / `action` 用 `valueEnum` 生成筛选下拉，再用 `render` 覆盖成彩色的 `StatusTag`——`valueEnum` 管筛选，`render` 管展示。

#### 7.2.3 表单：新建 / 编辑 / 参考配置三合一

```tsx
const [modalOpen, setModalOpen] = useState(false);
const [docOpen, setDocOpen] = useState(false);
const [editingRule, setEditingRule] = useState<Rule | null>(null);
const [isReference, setIsReference] = useState(false);
```

`ModalForm` 的标题由两个状态决定：

```tsx
title={isReference ? '参考配置（可修改后保存）' : editingRule?.id ? '编辑规则' : '新建规则'}
initialValues={editingRule || {}}
onFinish={async (values) => {
  if (editingRule?.id && !isReference) {
    await api.put(`/rules/${editingRule.id}`, values);
  } else {
    await api.post('/rules', values);       // 参考配置 → 当作新建
  }
  ...
}}
```

`openReference(type)` 把 `REFERENCE_CONFIG[type]` 预填进表单并置 `isReference=true`：

```tsx
const openReference = (type: keyof typeof REFERENCE_CONFIG) => {
  setEditingRule({ ...REFERENCE_CONFIG[type] } as Rule);
  setIsReference(true);
  setModalOpen(true);
};
```

**这是本项目最好的一个 UX 设计**：新手不知道表达式怎么写 → 点"参考：登录" → 得到一条可跑通的模板 → 微调后保存。三套模板分别在登录/订单/活动场景下用了 `eventType` + `patternType` + 计数阈值。

`modalProps={{ destroyOnClose: true }}` 是让表单在关闭时销毁、下次打开重新应用 `initialValues` 的关键；否则第二次点"编辑"会残留上一次的值（antd 5.25+ 该属性改名为 `destroyOnHidden`，升级会告警）。

#### 7.2.4 内置表达式文档

`EXPRESSION_DOC` 是一个纯前端的文档数据源，用三个 `Table` 展示"可用变量 / 运算符 / CEP 模式类型"。它的价值是把 `context` 契约"文档化"给了使用者——但**它自己有过期内容**（见第 10 章）：

- `=~` 的示例写成 `productId=~'ABNORMAL.*'`（右侧应为正则字面量 `/.../`）；
- `patterns` 里的窗口写成 "5s"，实际代码是 30~60s；
- `LOCATION_CHANGE` 描述为"IP 变化"，实际只匹配两次成功登录。

### 7.3 Risk（风险记录）

标准只读表格，`ProTable` 直接映射后端查询参数：

```tsx
request={async (params) => {
  const res = await api.get<PageResult<RiskRecord>>('/risks', {
    params: {
      page: params.current,
      pageSize: params.pageSize,
      userId: params.userId,
      eventType: params.eventType,
      riskLevel: params.riskLevel,
      action: params.action,
    },
  });
  return { data: res.data || [], success: true, total: res.total || 0 };
}}
```

- 可搜索列：`userId`（模糊）、`eventType`、`riskLevel`、`action`；
- `riskScore`、`createTime` 明确 `search: false`；
- **没有详情抽屉**：`details` 字段（JSON 字符串）在页面上完全没展示，想看命中规则只能查库。这是最值得补的功能。

### 7.4 Dashboard（数据看板）

```tsx
const { data: trend } = useTrend();          // /api/stats/trend
const { data: topRules } = useTopRules();    // /api/stats/top-rules

const dailyData = (trend?.dailyData || []).slice().sort((a, b) => b.date.localeCompare(a.date));
const totalEvents = dailyData.reduce((s, d) => s + d.total, 0);
const highEvents  = dailyData.reduce((s, d) => s + d.high, 0);
```

三个统计卡 + 两张 `Table`：

| 区块 | 数据源 | 说明 |
|------|--------|------|
| 7 天总风险 / 7 天高风险 / 今日命中规则数 | 前端 `reduce` / `topRules.length` | 由明细算汇总 |
| 风险趋势（近 7 天） | `/stats/trend` 的 `dailyData` | 倒序排列后表格展示 |
| 规则命中排行（今日） | `/stats/top-rules` | Top 10，前三名高亮序号 |

**没有图表库**（无 echarts/antv/recharts 依赖），README 里说的"风险趋势图"实际是表格。想加图表需要引入依赖。

`sort` 前先 `slice()` 是必须的：`trend.dailyData` 来自 hook 的 state，直接 `sort` 会原地修改并被 React 视为未变化（同类 bug 的高发点）。

---

## 8. 样式体系

三层：

| 层 | 文件 | 内容 |
|----|------|------|
| Design Token | `main.tsx` 的 `ConfigProvider.theme` | 主色 `#2563eb`、圆角、字体、组件级 token |
| 组件样式 | `components/*/*.styles.ts` 用 `antd-style` 的 `createStyles` | `StatCard` 的卡片、悬停、图标容器 |
| 布局样式 | `App.css` + `index.css` | `.app-layout`/`.app-sider`/`.app-logo`/`.app-content`，全局重置 |

`StatCard.styles.ts` 的写法：

```ts
export const useStyles = createStyles(({ token, css }) => ({
  card: css`
    border-radius: ${token.borderRadiusLG}px;
    border: 1px solid #f1f5f9;
    transition: box-shadow .25s ..., transform .25s ...;
    &:hover { box-shadow: 0 8px 32px rgba(0,0,0,.08); transform: translateY(-2px); }
  `,
  clickableCard: css` ... cursor: pointer; ... .ant-statistic-content-value { font-size: 32px; ... } `,
  icon: css` width: 52px; height: 52px; border-radius: 14px; background: #eff6ff; ... `,
}));
```

一个小不一致：`.ant-statistic-title` / `.ant-statistic-content-value` 的排版覆盖**只写在 `clickableCard` 里**，所以可点击卡片（Home）与不可点击卡片（Dashboard）的字体大小不同。若想统一，把那段嵌套规则抽到一个共享 class。

---

## 9. 与后端的字段契约

联调时对照这张表，能省掉大部分排查时间：

| 场景 | 前端 | 后端 | 转换点 |
|------|------|------|--------|
| 规则名 | `ruleName` | `name`（DB 列） | `Rule.syncFromFrontend/ToFrontend` |
| 规则表达式 | `ruleExpression` | `expression`（DB 列） | 同上 |
| 规则类型 | `ruleType` | **无对应列**，永远丢失 | 无 |
| 列表搜索 | `params.ruleName` | `name` | `ProTable.request` 手工映射 |
| 分页 | `params.current` / `params.pageSize` | `page` / `pageSize` | 同上 |
| 分页响应 | `{data,total}` | `{data,total,page,pageSize}` | 只用前两个 |
| 风险记录 | `riskLevel`/`action`/`eventType` | 同名 | 无 |
| `details` | `string`（未展示） | MySQL JSON 列 | 无 |
| 枚举大小写 | `HIGH`/`BLOCK` | `RiskLevel.valueOf("HIGH")` | **必须大写** |

两个必须记住的约定：

1. **规则保存时不传 `ruleType`**（传了也不存）；想区分场景只能靠命名，后端统计也是靠名字猜。
2. **`riskLevel` 必须是大写枚举名**。写成 `high` 会让 job 的 `RuleSourceFunction` 在 `valueOf` 处抛异常 → 整轮规则加载失败（不是这一条失效）→ 作业用旧规则或空规则（fail-open）。

---

## 10. 已知问题

| # | 问题 | 位置 | 影响 |
|---|------|------|------|
| 1 | `=~` 示例写成字符串字面量 | `Rule/List.tsx` 的 `EXPRESSION_DOC.operators` | 用户照着写规则必然失效 |
| 2 | 模式窗口写 "5s"，实际 30~60s | 同文件 `EXPRESSION_DOC.patterns` | 用户误判触发条件 |
| 3 | `LOCATION_CHANGE` 描述为"IP 变化" | 同上 | 与 CEP 实现不符 |
| 4 | 动作筛选不生效 | `Rule/List.tsx` 的 `request` 未传 `action` | 用户以为过滤了 |
| 5 | `services/` 三个文件是死代码 | `src/services/*` | 改接口容易改错地方 |
| 6 | `usePageRequest` 未使用 | `utils/request.ts` | 误导 |
| 7 | `as unknown as` 类型强转 | 两个列表页 | 拦截器与 axios 类型不一致的后遗症 |
| 8 | 请求失败无 UI 提示 | `utils/request.ts` + 各页面 | 只有 console，用户看到空白表格 |
| 9 | 开关操作无失败回滚 | `Rule/List.tsx` | 状态可能与库不一致 |
| 10 | `bordered={false}`、`destroyOnClose` 已废弃 | `StatCard`、`ModalForm` | 升级 antd 会告警 |
| 11 | 详情未展示 | `Risk/index.tsx` | 命中规则只能查库 |
| 12 | 无鉴权、无权限控制 | 全局 | 任何能访问 8080 的人都能改规则 |
| 13 | `dayjs`、`usePageRequest`、`services` 冗余 | — | 包体与认知负担 |

---

## 11. 读完后应该能回答的问题

1. `api` 拦截器为什么让页面必须写 `as unknown as PageResult<T>`？
2. `useRequest` 里为什么用 `requestFnRef` 而不是把 `requestFn` 放进依赖数组？
3. 为什么 `selectedKey` 要对 `/` 单独判断？
4. 规则页的后端参数 `name` 是怎么从 `params.ruleName` 来的？
5. 点"参考：订单"后保存，走的是 POST 还是 PUT？为什么？
6. 为什么 Dashboard 的 `sort` 之前要 `slice()`？
7. 新增一个 `action=FREEZE` 需要改几个地方？（`RiskAction` 枚举、`ACTION_CONFIG`、必要时后端枚举说明）
8. 如果后端把 `ruleName` 改成 `name` 返回，前端要改哪些文件？

### 动手练习

- **练习 1**：修复规则页动作筛选（把 `action` 加进 `request` 参数），验证筛选生效。
- **练习 2**：给风险记录页加"详情抽屉"，解析 `details` 字符串并展示 `matchedRuleName`、`failCount` 等字段。
- **练习 3**：把 `EXPRESSION_DOC` 的 `=~` 示例与模式窗口改成与代码一致，并在类型上抽出接口避免再写错。
- **练习 4**：给 `api` 加一个 `onError` 全局提示（`antd` 的 `message.error`），验证接口 500 时用户能看到反馈。
- **练习 5**：给 Dashboard 引入一个图表库，把 `dailyData` 画成折线图，并保留表格作为降级。

---

回到：[学习总览](../LEARNING-GUIDE.md) · [01 · common](01-flink-risk-common.md) · [02 · job](02-flink-risk-job.md) · [03 · web](03-flink-risk-web.md)
