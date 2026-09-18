# 查询工作空间内的 Slave

`GET /api/v1/slaves` 返回当前 Observer 绑定工作空间内的已注册 Slave，无需 Driver 在线，也不要求内部服务 token。

```bash
curl http://9.0.16.66:8080/api/v1/slaves
curl 'http://9.0.16.66:8080/api/v1/slaves?available_only=true'
```

默认包含离线节点；`available_only=true` 只返回租约有效的节点。相同 `slave_id` 的历史实例不会重复列出，优先选择在线实例，否则显示最近心跳的实例。结果按 `slave_id` 排序。

响应示例（描述符列表按实际注册信息返回）：

```json
{
  "workspace_id": "workspace-default",
  "slaves": [
    {
      "slave_id": "slave-a",
      "available": true,
      "lease_state": "active",
      "last_seen_at": "2026-09-18T13:03:56+00:00",
      "base_operations": ["run_code"],
      "executor_descriptors": [],
      "runtime_plugin_descriptors": [],
      "term_support": []
    }
  ]
}
```

- `available` 仅表示 Observer 最近收到有效心跳，租约尚未失效，不代表 CPU/内存充足或特定能力包已经激活；任务仍须通过 readiness/admission 检查。
- `lease_state` 区分 `active`、`expired`、`released`。
- `base_operations` 是注册时上报的基础操作，不包含通过遍历其他 Run 推导出的临时能力。执行器、runtime plugin 和 term 描述符同样来自 Slave 上报；离线时仅供参考。
- 不返回内部 endpoint、lease token/hash、实例标识或任意其他注册字段。目前没有 CPU/内存余量指标。
- 当前版本按 Observer 配置的 `workspace_id` 隔离，而非按登录用户做多租户授权。可选 `workspace_id` 参数只能等于此配置，否则返回 `403 workspace_binding_mismatch`；部署方仍需限制公共 API 的访问范围。
- 未注册任何 Slave 时返回 `{"workspace_id":"…","slaves":[]}`。接口和响应模型可从 `/docs` 或 `/openapi.json` 查看。

该接口是资源发现视图；已有 `/api/v1/capabilities` 继续提供原有的能力/激活视图，不改变其响应格式。更新代码后须重新部署 Observer 才会生效。
