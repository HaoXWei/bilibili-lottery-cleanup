#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Bilibili 过期抽奖转发动态全自动清理器
=============================================================================
核心特性：
1. 【完全无人值守流水线】：
   - 采用持续流式处理架构：
     「小步平滑推进加载 -> 增量识别过期 -> 防风控延时删除 -> 轮次小憩 -> 自动下一轮」
   - 持续运行直至动态历史最底端（出现「没有更多动态了」）或达成用户设定的 --limit。
2. 【智能防风控与人机保护 (Anti-Bot)】：
   - 平滑滚动推进 (Smooth Scroll)，步长与间隔引入随机微扰动 (Jitter)，避免机械特征。
   - 单条删除自带随机间隔；支持分批小憩与轮次休整机制。
   - 智能熔断降频：捕获 4101140 / -412 等频控提示时自动冷却休眠并自适应微调间隔，稳定不崩溃。
   - 极验滑块监测：自动检测页面是否弹出验证滑块，若出现则自动挂起等待，人工通过后无缝继续。
3. 【三大类型精准识别与未开奖保护】：
   - 条件一：源动态已被作者删除 / 源动态不可见（DOM 精准识别）。
   - 条件二：评论区抽奖且提取日期已过期（智能正则推算）。
   - 条件三：官方互动抽奖已开奖（官方 API lottery_notice 批量核验 status==2）。
   - 严格安全保护：仍在倒计时中的未开奖活动 (status==0) 与原创动态绝不误删。
4. 【DOM 轻量化优化】：
   - 成功删除后实时移除对应 DOM 节点，避免卡片堆积造成 Chrome 内存占用与卡顿。
   - 具备内存缓存池，已核验过的动态与抽奖状态绝不重复请求 API。

使用方法：
  python cleanup_lottery.py              # 全自动持续清理直至历史最底部
  python cleanup_lottery.py --limit 50   # 累计清理 50 条后自动停止
  python cleanup_lottery.py --scan       # 仅扫描输出清单，不执行任何删除
  python cleanup_lottery.py --delay 1.8  # 自定义单条删除基准间隔 (秒)
=============================================================================
"""

import argparse
import asyncio
import json
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
import requests
import websockets

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

NOW = datetime.now(tz=timezone(timedelta(hours=8)))
_msg_counter = 0

# ─────────────────────────────────────────────────────────────────────────────
# CDP 基础通信封装
# ─────────────────────────────────────────────────────────────────────────────

def get_bilibili_tab_ws(port=9222):
    """获取 Bilibili 动态页面的 WebSocket Debugger URL"""
    try:
        res = requests.get(f"http://127.0.0.1:{port}/json", timeout=3)
        tabs = res.json()
    except Exception:
        print(f"❌ 无法连接到 Chrome CDP 端口 (http://127.0.0.1:{port})")
        print("💡 请确认 Chrome 是否以调试模式启动，例如命令行执行：")
        print('   chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\\chrome-dev-profile"')
        return None

    for tab in tabs:
        url = tab.get("url", "")
        if "bilibili.com" in url and tab.get("type") == "page":
            return tab.get("webSocketDebuggerUrl")
    
    print("❌ 未在 Chrome 中找到打开的 Bilibili 页面，请先在 Chrome 中打开个人动态页：")
    print("   https://space.bilibili.com/你的UID/dynamic")
    return None

async def cdp_call(ws, method, params=None):
    """发送 CDP 原生指令并接收返回"""
    global _msg_counter
    _msg_counter += 1
    mid = _msg_counter
    req = {"id": mid, "method": method}
    if params:
        req["params"] = params
    await ws.send(json.dumps(req))
    while True:
        resp = json.loads(await ws.recv())
        if resp.get("id") == mid:
            return resp

async def cdp_eval(ws, expression, context_id=None):
    """在页面上下文执行 JavaScript 并返回结果"""
    global _msg_counter
    _msg_counter += 1
    mid = _msg_counter
    params = {
        "expression": expression,
        "returnByValue": True,
        "awaitPromise": True
    }
    if context_id:
        params["contextId"] = context_id
    req = {"id": mid, "method": "Runtime.evaluate", "params": params}
    await ws.send(json.dumps(req))
    while True:
        resp = json.loads(await ws.recv())
        if resp.get("id") == mid:
            return resp.get("result", {}).get("result", {}).get("value")

async def cdp_click(ws, x, y):
    """CDP 真实鼠标事件点击"""
    await cdp_call(ws, "Input.dispatchMouseEvent", {
        "type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1
    })
    await asyncio.sleep(0.05)
    await cdp_call(ws, "Input.dispatchMouseEvent", {
        "type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1
    })

# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数：日期推算、人机验证检测
# ─────────────────────────────────────────────────────────────────────────────

def extract_draw_date(text):
    """从文本中提取最晚的开奖日期 (XXXX年XX月XX日 或 XX月XX日)"""
    dates = []
    # 匹配完整年月日
    for m in re.finditer(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text):
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                          tzinfo=timezone(timedelta(hours=8)))
            dates.append(dt)
        except ValueError:
            pass
    # 匹配仅月日（默认当前年份）
    if not dates:
        current_year = NOW.year
        for m in re.finditer(r'(\d{1,2})月(\d{1,2})日', text):
            try:
                dt = datetime(current_year, int(m.group(1)), int(m.group(2)),
                              tzinfo=timezone(timedelta(hours=8)))
                dates.append(dt)
            except ValueError:
                pass
    return max(dates) if dates else None

async def check_and_wait_captcha(ws):
    """检查页面是否弹出极验/人机滑块，若弹出则挂起等待用户解决"""
    has_captcha = await cdp_eval(ws, """(() => {
        const el = document.querySelector('.geetest_holder, .geetest_popup, [class*="geetest"], [class*="captcha"]');
        return el ? (el.offsetWidth > 0 && el.offsetHeight > 0) : false;
    })()""")

    if has_captcha:
        print("\n" + "!" * 68)
        print("⚠️ 【安全防线触发】检测到浏览器窗口弹出人机验证/极验滑块！")
        print("💡 脚本已自动暂停挂起，请切换到 Chrome 浏览器窗口手动完成滑块拼图。")
        print("✅ 完成拼图后，脚本将自动感应并恢复清理...")
        print("!" * 68 + "\n")
        while True:
            await asyncio.sleep(3)
            cleared = await cdp_eval(ws, """(() => {
                const el = document.querySelector('.geetest_holder, .geetest_popup, [class*="geetest"], [class*="captcha"]');
                return !el || el.offsetWidth === 0 || el.offsetHeight === 0;
            })()""")
            if cleared:
                print("🎉 人机验证已通过！安全恢复流水线执行...\n")
                await asyncio.sleep(2)
                break

# ─────────────────────────────────────────────────────────────────────────────
# 滚动与页面加载控制（小步温和推进，杜绝触发人机判断，支持持续执行）
# ─────────────────────────────────────────────────────────────────────────────

async def load_next_batch_scroll(ws, steps=4, base_delay=2.0):
    """
    温和向下滑动推进页面，加载下一批历史动态。
    特点：
    - 采用小步长平滑滚动 (1000~1400px)，配合随机微扰动，杜绝机械特征
    - 严格检测是否真正触及动态历史最底部 (出现 .bili-dyn-list__no-more)
    - 达到步数或加载到新动态后即时返回，交由主流程清理，实现流式持续处理
    返回值: is_true_bottom (bool)
    """
    print(f"📜 正在向下滑动加载下一批动态 (平滑推进 {steps} 步，模拟真人浏览)...")
    
    start_info = await cdp_eval(ws, """(() => {
        const items = document.querySelectorAll('.bili-dyn-list__item');
        const noMore = !!document.querySelector('.bili-dyn-list__no-more');
        return { count: items.length, noMore: noMore };
    })()""")
    
    if start_info and start_info.get('noMore'):
        print("🏁 页面已显示「没有更多动态了」，已达历史最底部。")
        return True

    start_count = start_info.get('count', 0) if start_info else 0

    for step in range(1, steps + 1):
        await check_and_wait_captcha(ws)
        scroll_step = random.randint(1000, 1400)
        
        state = await cdp_eval(ws, f"""(() => {{
            window.scrollBy({{ top: {scroll_step}, behavior: 'smooth' }});
            // 派发 scroll 事件确保 B 站 IntersectionObserver / 滚动监听器正常响应
            window.dispatchEvent(new Event('scroll'));
            const items = document.querySelectorAll('.bili-dyn-list__item');
            const noMore = !!document.querySelector('.bili-dyn-list__no-more');
            const atBottom = (window.innerHeight + window.scrollY) >= (document.documentElement.scrollHeight - 300);
            return {{ count: items.length, noMore: noMore, atBottom: atBottom }};
        }})()""")

        count = state.get('count', 0) if state else 0
        no_more = state.get('noMore', False) if state else False

        if no_more:
            print("🏁 检测到「没有更多动态了」，已达历史最底端。")
            return True

        # 若已经加载了较多新动态（超过 15 条），则提前结束本次滚动，进入清理阶段
        if count >= start_count + 15:
            print(f"   [推进中] 已成功加载新批次动态 (当前共 {count} 条)")
            break

        # 人性化随机间隔
        jitter = random.uniform(0.2, 0.6)
        await asyncio.sleep(base_delay + jitter)

    # 再次检查是否到达底部或需要轻微抖动触发加载
    final_state = await cdp_eval(ws, """(() => {
        const items = document.querySelectorAll('.bili-dyn-list__item');
        const noMore = !!document.querySelector('.bili-dyn-list__no-more');
        const atBottom = (window.innerHeight + window.scrollY) >= (document.documentElement.scrollHeight - 300);
        return { count: items.length, noMore: noMore, atBottom: atBottom };
    })()""")

    if final_state and final_state.get('noMore'):
        return True

    # 如果已经滚到了当前可视区底部但未出现新动态，尝试轻微上下回弹触发 B 站请求
    if final_state and final_state.get('atBottom') and final_state.get('count') == start_count:
        print("   ⏳ 正在等待 B 站服务端返回历史数据...")
        await cdp_eval(ws, "window.scrollBy({ top: -200, behavior: 'smooth' });")
        await asyncio.sleep(1.0)
        await cdp_eval(ws, "window.scrollBy({ top: 300, behavior: 'smooth' });")
        await asyncio.sleep(2.5)
        
        post_nudge = await cdp_eval(ws, """(() => {
            const items = document.querySelectorAll('.bili-dyn-list__item');
            const noMore = !!document.querySelector('.bili-dyn-list__no-more');
            return { count: items.length, noMore: noMore };
        })()""")
        if post_nudge and post_nudge.get('noMore'):
            return True

    return False

# ─────────────────────────────────────────────────────────────────────────────
# 极速检测逻辑：DOM 扫描 + 官方 API 极速判断
# ─────────────────────────────────────────────────────────────────────────────

async def get_csrf_token(ws):
    """从浏览器 Cookie 中提取 bili_jct (CSRF token)"""
    return await cdp_eval(ws, """(() => {
        const match = document.cookie.match(/bili_jct=([^;]+)/);
        return match ? match[1] : '';
    })()""")

async def collect_page_dynamics(ws):
    """从页面 DOM 采集全部动态数据结构（包含 Vue orig 数据）"""
    return await cdp_eval(ws, """(() => {
        const items = document.querySelectorAll('.bili-dyn-list__item');
        const list = [];
        items.forEach((el, idx) => {
            const walk = (node, d) => {
                if (d > 3) return null;
                if (node.__vue__) return node.__vue__;
                for (const c of node.children) {
                    const v = walk(c, d+1);
                    if (v) return v;
                }
                return null;
            };
            const vue = walk(el, 0);
            const data = vue?.$props?.data;
            const dynId = data?.id_str || '';
            const dynType = data?.type || '';
            const origId = data?.orig?.id_str || '';
            const text = el.innerText || '';
            const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
            const link = el.querySelector('a[data-type="lottery"]');

            list.push({
                idx: idx,
                dynId: dynId,
                origId: origId,
                dynType: dynType,
                date: lines[1] || '',
                isDeleted: text.includes('源动态已被作者删除'),
                isInvisible: text.includes('源动态不可见'),
                isForward: text.includes('转发动态') || dynType === 'DYNAMIC_TYPE_FORWARD',
                hasLotteryTag: !!link,
                hasLotteryKeyword: /抽奖|开奖|中奖/.test(text),
                preview: lines.slice(0, 4).join(' | '),
                fullText: text
            });
        });
        return list;
    })()""")

async def batch_check_interactive_lotteries(ws, lottery_items, lottery_cache):
    """
    通过官方 HTTP API (lottery_svr/lottery_notice?business_type=4) 极速核验
    利用本地缓存机制，已查询过的抽奖活动不再重复发起网络请求
    返回字典: { origId: 'expired' | 'active' | 'unknown' }
    """
    to_query = []
    for it in lottery_items:
        oid = it.get('origId')
        if oid and oid not in lottery_cache:
            to_query.append(oid)
    
    # 去重
    to_query = list(set(to_query))

    if to_query:
        print(f"⚡ 正在核验 {len(to_query)} 个抽奖活动的官方开奖结果 (API批量并发)...")
        js_batch_fetch = f"""
        (async () => {{
            const ids = {json.dumps(to_query)};
            const results = {{}};
            const batchSize = 5;
            for (let i = 0; i < ids.length; i += batchSize) {{
                const chunk = ids.slice(i, i + batchSize);
                await Promise.all(chunk.map(async (origId) => {{
                    try {{
                        const url = 'https://api.vc.bilibili.com/lottery_svr/v1/lottery_svr/lottery_notice?business_type=4&business_id=' + origId;
                        const res = await fetch(url, {{ credentials: 'include' }});
                        const data = await res.json();
                        if (data.code === 0 && data.data) {{
                            const status = data.data.status;
                            // status: 2 -> 已开奖, status: 0 -> 未开奖 (倒计时中)
                            if (status === 2) results[origId] = 'expired';
                            else if (status === 0) results[origId] = 'active';
                            else results[origId] = 'unknown';
                        }} else {{
                            results[origId] = 'unknown';
                        }}
                    }} catch(e) {{
                        results[origId] = 'unknown';
                    }}
                }}));
                await new Promise(r => setTimeout(r, 80));
            }}
            return results;
        }})()
        """
        fetched = await cdp_eval(ws, js_batch_fetch)
        if fetched:
            lottery_cache.update(fetched)

    return lottery_cache

# ─────────────────────────────────────────────────────────────────────────────
# 极速删除与防风控控制 (API 优先，UI 兜底，自动自适应降频)
# ─────────────────────────────────────────────────────────────────────────────

async def api_remove_dynamic(ws, csrf, dyn_id):
    """
    调用官方 remove API 删除动态
    返回结果字典: {"ok": bool, "rate_limited": bool, "code": int, "msg": str}
    """
    js_del = f"""
    (async () => {{
        try {{
            const res = await fetch('https://api.bilibili.com/x/dynamic/feed/operate/remove?csrf=' + '{csrf}', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json;charset=UTF-8' }},
                body: JSON.stringify({{ dyn_id_str: '{dyn_id}' }}),
                credentials: 'include'
            }});
            const data = await res.json();
            const msg = data.message || data.msg || '';
            const isRateLimit = res.status === 412 || res.status === 429 ||
                                data.code === -412 || data.code === 4101140 || data.code === 500001 ||
                                /频繁|过快|稍后|拦截|限制|歇/.test(msg);

            if (data.code === 0 || data.code === 4101142 || data.code === 4101152) {{
                // 成功删除或服务端已删除，实时从 DOM 移除该卡片，轻量化页面
                const items = document.querySelectorAll('.bili-dyn-list__item');
                for (const el of items) {{
                    const walk = (node, d) => {{
                        if (d > 3) return null;
                        if (node.__vue__) return node.__vue__;
                        for (const c of node.children) {{ const v = walk(c, d+1); if(v) return v; }}
                        return null;
                    }};
                    const vue = walk(el, 0);
                    if (vue?.$props?.data?.id_str === '{dyn_id}') {{
                        el.remove();
                        break;
                    }}
                }}
                return {{ ok: true, rate_limited: false, code: data.code, msg: msg }};
            }}
            return {{ ok: false, rate_limited: isRateLimit, code: data.code, msg: msg }};
        }} catch(e) {{
            return {{ ok: false, rate_limited: false, code: -1, msg: e.message }};
        }}
    }})()
    """
    res = await cdp_eval(ws, js_del)
    return res or {"ok": False, "rate_limited": False, "code": -1, "msg": "CDP通信异常"}

async def ui_remove_dynamic_fallback(ws, dyn_id, label=""):
    """
    兜底方案：CDP 真实鼠标点击 ⋯ -> 删除 -> 确认删除
    """
    scrolled = await cdp_eval(ws, f"""(() => {{
        const items = document.querySelectorAll('.bili-dyn-list__item');
        for (const el of items) {{
            const walk = (node, d) => {{
                if (d > 3) return null;
                if (node.__vue__) return node.__vue__;
                for (const c of node.children) {{ const v = walk(c, d+1); if(v) return v; }}
                return null;
            }};
            const vue = walk(el, 0);
            if (vue?.$props?.data?.id_str === '{dyn_id}') {{
                el.scrollIntoView({{ block: 'center' }});
                return true;
            }}
        }}
        return false;
    }})()""")
    if not scrolled:
        return False

    await asyncio.sleep(0.4)

    btn_pos = await cdp_eval(ws, f"""(() => {{
        const items = document.querySelectorAll('.bili-dyn-list__item');
        for (const el of items) {{
            const walk = (node, d) => {{
                if (d > 3) return null;
                if (node.__vue__) return node.__vue__;
                for (const c of node.children) {{ const v = walk(c, d+1); if(v) return v; }}
                return null;
            }};
            const vue = walk(el, 0);
            if (vue?.$props?.data?.id_str === '{dyn_id}') {{
                const btn = el.querySelector('.bili-dyn-more__btn');
                if (!btn) return null;
                const r = btn.getBoundingClientRect();
                return {{ x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2) }};
            }}
        }}
        return null;
    }})()""")
    if not btn_pos:
        return False

    await cdp_click(ws, btn_pos['x'], btn_pos['y'])
    await asyncio.sleep(0.6)

    del_pos = await cdp_eval(ws, f"""(() => {{
        const items = document.querySelectorAll('.bili-dyn-list__item');
        for (const el of items) {{
            const walk = (node, d) => {{
                if (d > 3) return null;
                if (node.__vue__) return node.__vue__;
                for (const c of node.children) {{ const v = walk(c, d+1); if(v) return v; }}
                return null;
            }};
            const vue = walk(el, 0);
            if (vue?.$props?.data?.id_str === '{dyn_id}') {{
                for (const l of el.querySelectorAll('.bili-cascader-options__item-label')) {{
                    if (l.innerText.trim() === '删除') {{
                        const r = l.getBoundingClientRect();
                        return {{ x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2) }};
                    }}
                }}
            }}
        }}
        return null;
    }})()""")
    if not del_pos:
        await cdp_eval(ws, "document.body.click()")
        return False

    await cdp_click(ws, del_pos['x'], del_pos['y'])
    await asyncio.sleep(0.6)

    confirm_pos = await cdp_eval(ws, """(() => {
        const btn = document.querySelector('.bili-modal__button.confirm');
        if (btn) {
            const r = btn.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) {
                return { x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2) };
            }
        }
        return null;
    })()""")
    if not confirm_pos:
        await cdp_call(ws, "Input.dispatchKeyEvent", {"type": "keyDown", "key": "Escape"})
        return False

    await cdp_click(ws, confirm_pos['x'], confirm_pos['y'])
    await asyncio.sleep(1.0)
    return True

# ─────────────────────────────────────────────────────────────────────────────
# 持续流水线核心主循环 (外层无限循环，直至历史彻底触底或达成 limit)
# ─────────────────────────────────────────────────────────────────────────────

async def run(args):
    ws_url = get_bilibili_tab_ws(args.port)
    if not ws_url:
        return

    print("=" * 70)
    print("🚀 Bilibili 过期抽奖转发动态清理器")
    print(f"🕐 当前时间: {NOW.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚙️ 运行参数: 单条间隔={args.delay}s, 分批小憩={args.batch_size}条/休{args.batch_rest}s, 轮次休整={args.pass_rest}s")
    if args.limit > 0:
        print(f"🎯 目标清理上限: {args.limit} 条")
    else:
        print("🎯 运行目标: 全自动持续运行直至个人全部历史动态底端 (无轮数上限)")
    print(f"🔗 WebSocket: {ws_url[:70]}...")
    print("=" * 70)

    async with websockets.connect(ws_url, max_size=100*1024*1024) as ws:
        csrf = await get_csrf_token(ws)
        if not csrf:
            print("⚠️ 未获取到 bili_jct Cookie，将降级为全流程 UI 模拟点击方式。")
        else:
            print(f"🔑 CSRF Token 获取成功: {csrf[:6]}******")

        pass_no = 0
        total_cleaned = 0
        total_failed = 0
        overall_start_time = time.time()
        current_delay = args.delay
        
        # 内存记录集，避免跨轮次重复处理已保留项与重复查询抽奖 API
        processed_dyn_ids = set()
        lottery_cache = {}

        # 核心全自动流式循环：持续执行，无需人工干预
        while True:
            pass_no += 1
            print("\n" + "=" * 70)
            print(f"🔄 【流水线第 {pass_no} 轮】正在检查当前页面动态并平滑加载...")
            print("=" * 70)

            # 1. 滚动加载当前批次
            is_bottom = False
            if not args.no_scroll:
                is_bottom = await load_next_batch_scroll(ws, steps=args.scroll_steps, base_delay=args.scroll_delay)
            else:
                print("⏩ 参数 --no-scroll: 已跳过滚动加载，仅处理当前页面已渲染的动态。")
                is_bottom = True

            # 2. 收集当前 DOM 动态元数据
            raw_items = await collect_page_dynamics(ws)
            if not raw_items:
                print("⚠️ 页面未找到任何动态卡片，休整 3 秒后重试...")
                await asyncio.sleep(3)
                continue

            # 过滤出本轮尚未决断处理的新动态
            new_items = [it for it in raw_items if it.get('dynId') and it['dynId'] not in processed_dyn_ids]
            print(f"📊 DOM 卡片总计: {len(raw_items)} 条 | 本轮待分析新动态: {len(new_items)} 条")

            # 3. 智能三分类识别
            candidates_del = []     # (dynId, reason, preview)
            interactive_items = []  # 互动抽奖集合

            for it in new_items:
                dyn_id = it['dynId']
                
                # 原创动态或非转发正常内容：直接标记保留，绝不误触
                if not it['isForward'] and not it['isDeleted'] and not it['isInvisible']:
                    processed_dyn_ids.add(dyn_id)
                    continue

                # 条件一：源动态已被作者删除 或 源动态不可见 (基于 DOM 精准识别)
                if it['isDeleted']:
                    candidates_del.append((dyn_id, '源动态已被作者删除', it['preview']))
                    continue
                if it['isInvisible']:
                    candidates_del.append((dyn_id, '源动态不可见', it['preview']))
                    continue

                # 条件二：评论区抽奖 (基于正则推算截止日期)
                if it['hasLotteryKeyword'] and not it['hasLotteryTag']:
                    draw_date = extract_draw_date(it['fullText'])
                    if draw_date and draw_date < NOW:
                        reason = f"评论区抽奖已过期({draw_date.strftime('%Y-%m-%d')})"
                        candidates_del.append((dyn_id, reason, it['preview']))
                        continue
                    elif draw_date and draw_date >= NOW:
                        # 仍未到截止日期：保留
                        processed_dyn_ids.add(dyn_id)
                        continue

                # 条件三：官方互动抽奖
                if it['hasLotteryTag']:
                    d = it['date']
                    # 历史往年抽奖 (如 2018~2025年)，早已过期开奖，直接归入删除
                    if any(f"{y}年" in d for y in range(2018, NOW.year)):
                        candidates_del.append((dyn_id, f'历史往年抽奖({d})', it['preview']))
                    else:
                        # 近期当年的互动抽奖：收集起来统一 API 极速核验
                        interactive_items.append(it)
                    continue

                # 其他未知普通转发：保留
                processed_dyn_ids.add(dyn_id)

            # 4. 针对近期互动抽奖批量查询官方状态 (status==2已开奖删除, status==0未开奖保护)
            if interactive_items:
                await batch_check_interactive_lotteries(ws, interactive_items, lottery_cache)
                for it in interactive_items:
                    dyn_id = it['dynId']
                    orig_id = it.get('origId')
                    status = lottery_cache.get(orig_id, 'unknown')
                    if status == 'expired':
                        candidates_del.append((dyn_id, '互动抽奖已开奖(API确认)', it['preview']))
                    elif status == 'active':
                        # 严密保护：未开奖倒计时中！
                        processed_dyn_ids.add(dyn_id)
                    else:
                        # 兜底：状态未知保留
                        processed_dyn_ids.add(dyn_id)

            print(f"📋 第 {pass_no} 轮识别完成：待清理过期动态: {len(candidates_del)} 条")

            # 5. 如果是 --scan 预览模式
            if args.scan:
                if candidates_del:
                    print(f"\n💡 [预览模式] 第 {pass_no} 轮待删除动态清单 (不执行删除)：")
                    for i, (dyn_id, reason, preview) in enumerate(candidates_del):
                        print(f"   [{i+1:03d}] [{reason}] {preview}")
                        processed_dyn_ids.add(dyn_id)
                else:
                    print("✨ 本轮未发现待清理的过期抽奖动态。")

                if is_bottom or args.no_scroll:
                    print("\n🏁 已触及历史动态最底端，扫描预览结束。")
                    break

                print(f"☕ 预览小憩 2 秒后自动进入下一轮扫描...")
                await asyncio.sleep(2)
                continue

            # 6. 执行本轮删除 (带有随机微扰防人机与自适应熔断冷却)
            if candidates_del:
                to_execute = candidates_del
                if args.limit and args.limit > 0:
                    remaining_quota = args.limit - total_cleaned
                    if remaining_quota <= 0:
                        print(f"\n🎯 已达到设定的清理目标 ({args.limit} 条)，任务圆满完成！")
                        break
                    to_execute = candidates_del[:remaining_quota]

                print(f"\n⚡ 本轮即将自动清理 {len(to_execute)} 条动态...")
                batch_deleted = 0
                consecutive_fails = 0

                for i, (dyn_id, reason, preview) in enumerate(to_execute):
                    await check_and_wait_captcha(ws)
                    print(f"[{i+1:03d}/{len(to_execute):03d}] 删除: {preview[:38]}", end='', flush=True)

                    max_retries = 3
                    del_ok = False
                    last_msg = ""

                    for attempt in range(max_retries):
                        res = await api_remove_dynamic(ws, csrf, dyn_id) if csrf else {"ok": False, "rate_limited": False}

                        if res.get("ok"):
                            del_ok = True
                            consecutive_fails = 0
                            print(" -> ✅ 成功")
                            break

                        if res.get("rate_limited"):
                            last_msg = res.get("msg") or "操作速度过快"
                            cooldown = 20 + attempt * 10 + random.randint(2, 6)
                            print(f"\n   ⏳ [防风控冷却] B站提示: {last_msg}。进入安全休眠 {cooldown} 秒 (第 {attempt+1}/{max_retries} 次)...", flush=True)
                            # 自适应上调基准延时，降低后续触发概率
                            current_delay = max(current_delay + 0.3, 1.8)
                            await asyncio.sleep(cooldown)
                            continue

                        # 非频率问题，尝试 UI 模拟点击兜底一次
                        print(" (API跳过，尝试UI)", end='', flush=True)
                        ui_ok = await ui_remove_dynamic_fallback(ws, dyn_id, preview)
                        if ui_ok:
                            del_ok = True
                            consecutive_fails = 0
                            print(" -> ✅ UI删除成功")
                            break
                        else:
                            last_msg = res.get("msg") or "未知错误"
                            break

                    processed_dyn_ids.add(dyn_id)

                    if del_ok:
                        total_cleaned += 1
                        batch_deleted += 1
                        
                        # 分批小憩机制：每连续删除 N 条，稍作停顿平抑请求频次
                        if args.batch_size > 0 and batch_deleted % args.batch_size == 0 and (i + 1) < len(to_execute):
                            rest_time = args.batch_rest + random.uniform(1.0, 2.5)
                            print(f"   ☕ 已连续处理 {batch_deleted} 条，自动小憩 {rest_time:.1f} 秒平抑请求频率...", flush=True)
                            await asyncio.sleep(rest_time)
                    else:
                        total_failed += 1
                        consecutive_fails += 1
                        print(f" -> ⚠️ 失败 ({last_msg})")

                        # 连续失败熔断保护
                        if consecutive_fails >= 4:
                            print(f"\n🛡️ 连续出现 {consecutive_fails} 次异常，触发深度安全休眠 30 秒...", flush=True)
                            await asyncio.sleep(30)
                            consecutive_fails = 0

                    # 单条删除间隔（引入随机微扰，杜绝固定机械节奏）
                    del_jitter = random.uniform(0.2, 0.6)
                    await asyncio.sleep(current_delay + del_jitter)

                if args.limit and total_cleaned >= args.limit:
                    print(f"\n🎯 已达到设定的清理目标 ({args.limit} 条)，任务圆满完成！")
                    break
            else:
                print("✨ 本轮暂无待清理的过期抽奖动态。")

            # 7. 触底终止判定与自动进入下一轮
            if is_bottom or args.no_scroll:
                # 再次确认页面是否还有未清理项
                print("\n🎉 全部历史动态已扫描完毕，已成功触达个人动态最底端！")
                break

            # 轮次间小憩（平滑过渡，给 B 站接口留出滑动窗口恢复时间）
            pass_rest_actual = args.pass_rest + random.uniform(0.5, 1.5)
            print(f"\n☕ 第 {pass_no} 轮完成（累计已清理: {total_cleaned} 条）。休整 {pass_rest_actual:.1f} 秒后自动滚动下一轮历史动态...")
            await asyncio.sleep(pass_rest_actual)

        elapsed = time.time() - overall_start_time
        print("\n" + "=" * 70)
        print(f"🎉 动态清理任务结束！")
        print(f"📊 累计成功删除: {total_cleaned} 条")
        print(f"⚠️ 失败/跳过: {total_failed} 条")
        print(f"⏱️ 总耗时: {elapsed/60:.1f} 分钟 ({elapsed:.1f} 秒)")
        print("=" * 70)

def main():
    parser = argparse.ArgumentParser(description="Bilibili 过期抽奖转发动态全自动清理器")
    parser.add_argument("--scan", action="store_true", help="仅扫描分类并输出清单，不执行任何实际删除")
    parser.add_argument("--no-scroll", action="store_true", help="不自动滚动到底部，仅处理当前页面已渲染的动态")
    parser.add_argument("--limit", type=int, default=0, help="最多删除动态条数 (默认 0 表示不限制，持续清理直到历史最底部)")
    parser.add_argument("--scroll-steps", type=int, default=4, help="每轮推进滚动步数，小步平滑推进避免人机滑块 (默认 4 步)")
    parser.add_argument("--scroll-delay", type=float, default=2.0, help="每步滚动后的等待时间 (秒，默认 2.0 秒)")
    parser.add_argument("--delay", type=float, default=1.5, help="删除单条后的基础间隔时间 (秒，默认 1.5 秒，自带随机浮动)")
    parser.add_argument("--batch-size", type=int, default=12, help="每连续删除多少条后进入分批小憩 (默认 12 条)")
    parser.add_argument("--batch-rest", type=float, default=5.0, help="分批小憩基础等待时间 (秒，默认 5.0 秒)")
    parser.add_argument("--pass-rest", type=float, default=4.0, help="每轮批次清理完毕后的轮次休整时间 (秒，默认 4.0 秒)")
    parser.add_argument("--port", type=int, default=9222, help="Chrome 远程调试端口 (默认 9222)")
    args = parser.parse_args()

    asyncio.run(run(args))

if __name__ == "__main__":
    main()
