#!/usr/bin/env python3
"""Generate excoredum UI design mockup as an SVG image.

Dark-theme design reference for building a UI on top of the deterministic CQRS
matching engine. Every region is annotated with the engine command / read-side
query that backs it, so developers can follow the feature -> API mapping.
"""

import html
import textwrap
import random

ELEMS = []

BG = "#0e1116"
BOARD = "#141a21"
PANEL = "#1a212a"
CARD = "#20272f"
LINE = "#2c3742"
TXT = "#e6edf3"
MUTED = "#8795a3"
DIM = "#5c6a77"
GREEN = "#22c55e"
GREEN_D = "#132a1c"
RED = "#ef4444"
RED_D = "#2a1416"
CYAN = "#38bdf8"
CYAN_D = "#102433"
AMBER = "#f59e0b"
PURPLE = "#a78bfa"
PURPLE_D = "#1f1a30"

FONT = "ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"


def esc(s):
    return html.escape(str(s), quote=True)


def wtxt(s, size, mono=False):
    return int(len(str(s)) * size * (0.60 if mono else 0.56))


def rect(x, y, w, h, fill=None, stroke=None, rx=6, sw=1, opacity=None):
    a = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}"']
    if fill:
        a.append(f' fill="{fill}"')
    if stroke:
        a.append(f' stroke="{stroke}" stroke-width="{sw}"')
    if opacity is not None:
        a.append(f' opacity="{opacity}"')
    a.append("/>")
    ELEMS.append("".join(a))


def rrect(x, y, w, h, fill=None, stroke=None, rx=6, sw=1):
    a = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}"']
    if fill:
        a.append(f' fill="{fill}"')
    if stroke:
        a.append(f' stroke="{stroke}" stroke-width="{sw}"')
    a.append("/>")
    ELEMS.append("".join(a))


def text(x, y, s, size=12, fill=TXT, weight=400, family=FONT, anchor="start",
         spacing=None):
    a = [f'<text x="{x:.1f}" y="{y:.1f}" font-family="{family}" font-size="{size}"']
    a.append(f' fill="{fill}" font-weight="{weight}" text-anchor="{anchor}"')
    if spacing is not None:
        a.append(f' letter-spacing="{spacing}"')
    a.append(f'>{esc(s)}</text>')
    ELEMS.append("".join(a))


def panel(x, y, w, h, title, subtitle=None, accent=CYAN):
    rect(x, y, w, h, fill=PANEL, stroke=LINE, rx=8)
    rect(x, y, 4, h, fill=accent, rx=2)
    text(x + 16, y + 22, title, size=13, weight=700)
    if subtitle:
        text(x + 16, y + 38, subtitle, size=10.5, fill=MUTED)


def chip(x, y, w, h, label_, value, val_fill=TXT, bg=CARD, sub=None):
    rect(x, y, w, h, fill=bg, stroke=LINE, rx=8)
    text(x + 10, y + 15, label_, size=9.5, fill=MUTED)
    text(x + 10, y + 35, value, size=15, weight=700, fill=val_fill, family=MONO)
    if sub:
        text(x + 10, y + 49, sub, size=9, fill=DIM)


def thead(x, y, cols, widths, aligns):
    ax = x
    for c, w, a in zip(cols, widths, aligns):
        if a == "start":
            tx = ax
        elif a == "end":
            tx = ax + w
        else:
            tx = ax + w / 2
        text(tx, y, c.upper(), size=9, fill=MUTED, weight=600,
             anchor="start" if a == "start" else "end" if a == "end" else "middle")
        ax += w
    rrect(x, y - 8, sum(widths), 1, fill=LINE)


def trow(x, y, h, widths, cells, aligns, mono=True, size=11, fill=TXT, bg=None):
    if bg:
        rect(x, y, sum(widths), h, fill=bg, rx=3)
    ax = x
    for c, w, a in zip(cells, widths, aligns):
        if a == "start":
            tx = ax + 2
        elif a == "end":
            tx = ax + w - 2
        else:
            tx = ax + w / 2
        text(tx, y + h / 2 + 4, c, size=size, fill=fill, family=MONO if mono else FONT,
             anchor="start" if a == "start" else "end" if a == "end" else "middle")
        ax += w


def titlebar(x, y, w, h, title, tabs, status, status_fill):
    rect(x, y, w, h, fill=BOARD, rx=8)
    text(x + 18, y + h / 2 + 5, title, size=16, weight=800)
    ax = x + 340
    for t, active in tabs:
        tw = 82
        if active:
            rect(ax, y + 12, tw, 30, fill=CARD, stroke=LINE, rx=6)
        text(ax + tw / 2, y + h / 2 + 5, t, size=11.5, weight=650,
             fill=TXT if active else MUTED, anchor="middle")
        ax += tw + 8
    sw_ = 168
    rect(x + w - sw_ - 18, y + 12, sw_, 30, fill=status_fill, rx=15)
    text(x + w - sw_ - 18 + sw_ / 2, y + h / 2 + 5, status, size=11.5,
         weight=700, anchor="middle")


def footnote(x, y, w, lines):
    hh = 14 + 18 * len(lines)
    rect(x, y, w, hh, fill="#10151b", stroke=LINE, rx=6)
    text(x + 12, y + 18, "FEATURE -> API MAPPING", size=9.5, weight=700, fill=AMBER)
    # fix: draw the label with arrow coloring
    for i, ln in enumerate(lines):
        ty = y + 36 + i * 17
        if " <- " in ln:
            left, api = ln.split(" <- ", 1)
            text(x + 12, ty, left, size=10.5, fill=MUTED)
            ox = x + 12 + wtxt(left, 10.5)
            text(ox, ty, " <- ", size=10.5, fill=CYAN, weight=700)
            text(ox + wtxt(" <- ", 10.5), ty, api, size=10.5, fill=TXT)
        else:
            text(x + 12, ty, ln, size=10.5, fill=MUTED)


def book_rows(x, y, w, rows, side, max_total, row_h=17):
    for px, size, total in rows:
        frac = total / max_total if max_total else 0
        fill = RED_D if side == "ASK" else GREEN_D
        if side == "ASK":
            rect(x + w - frac * w, y, frac * w, row_h, fill=fill, rx=2)
        else:
            rect(x, y, frac * w, row_h, fill=fill, rx=2)
        # price, size, total
        c1 = f"{px:,.1f}" + ("  " + f"{size:.2f}" + "  " + f"{total:.2f}")
        _cell3(x, y, w, row_h, [f"{px:,.1f}", f"{size:.2f}", f"{total:.2f}"],
               [w * 0.40, w * 0.30, w * 0.30])
        y += row_h + 1
    return y


def _cell3(x, y, w, h, cells, widths):
    ax = x
    for c, wide in zip(cells, widths):
        text(ax + wide - 2, y + h / 2 + 4, c, size=11, fill=TXT, family=MONO, anchor="end")
        ax += wide


# ------------------------------------------------------------------ screens
def screen_trading(ox, oy, W, H):
    titlebar(ox + 6, oy + 6, W - 12, 54, "Trading Terminal  ·  BTC/USDT",
             [("Markets", False), ("Spot", True), ("Order", False),
              ("Portfolio", False), ("Admin", False), ("Ops", False)],
             "LEADER · APPLIED_POS 1,204,118", CYAN_D)
    cx, cy = ox + 24, oy + 74
    cw, ch = W - 48, H - 74 - 64
    bfh = 64

    obw = 320
    panel(cx, cy, obw, ch - bfh, "Order Book — L2",
          "readClient.orderBook(symbolId, maxLevels) · depth", CYAN)
    ix, iy = cx + 12, cy + 48
    iw = obw - 24
    tcol = [iw * 0.40, iw * 0.30, iw * 0.30]
    thead(ix, iy + 6, ["PRICE", "SIZE", "TOTAL"], tcol, ["end", "end", "end"])
    asks = [(68419.0, 0.30, 0.30), (68417.0, 0.30, 0.60), (68414.5, 0.80, 1.40),
            (68413.0, 2.60, 4.00), (68412.5, 1.24, 5.24)]
    max_a = max(a[2] for a in asks)
    ry = iy + 24
    for row in asks:
        ry = book_rows(ix, ry, iw, [row], "ASK", max_a)
    rect(ix, ry, iw, 24, fill="#223041", rx=4)
    text(ix + iw / 2, ry + 16, "BID 68411.0  ·  SPREAD 1.5", size=10.5, weight=700,
         fill=AMBER, anchor="middle", family=MONO)
    ry += 30
    bids = [(68407.5, 0.20, 0.20), (68408.0, 2.00, 2.20), (68409.5, 0.75, 2.95),
            (68410.5, 1.05, 4.00), (68411.0, 0.50, 4.50)]
    max_b = max(b[2] for b in bids)
    for row in bids:
        ry = book_rows(ix, ry, iw, [row], "BID", max_b)

    cxx = cx + obw + 20
    chw = 640
    panel(cxx, cy, chw, 330, "Price Chart",
          "aggregated from readClient.marketTrades(symbolId) + L2", CYAN)
    draw_candles(cxx + 16, cy + 60, chw - 32, 250)
    panel(cxx, cy + 350, chw, ch - bfh - 350, "Market Trade Tape",
          "readClient.marketTrades(symbolId, limit) · realtime", PURPLE)
    tx, ty = cxx + 14, cy + 350 + 44
    tw = chw - 28
    twc = [tw * 0.24, tw * 0.32, tw * 0.24, tw * 0.20]
    thead(tx, ty + 6, ["TIME", "PRICE", "SIZE", "SIDE"], twc,
          ["start", "end", "end", "start"])
    ty += 22
    tape = [("10:41:02.118", "68411.0", "0.25", "BUY"),
            ("10:41:01.996", "68412.5", "0.80", "SELL"),
            ("10:41:01.731", "68410.5", "1.05", "BUY"),
            ("10:41:01.503", "68413.0", "2.60", "SELL"),
            ("10:41:01.288", "68411.0", "0.50", "BUY")]
    for t_, p_, s_, sd in tape:
        cc = GREEN if sd == "BUY" else RED
        trow(tx, ty, 20, twc, [t_, p_, s_, ""], ["start", "end", "end", "start"],
             mono=True, size=11, fill=TXT, bg=None)
        # color the side cell
        sx = tx + twc[0] + twc[1] + twc[2] + 2
        text(sx, ty + 14, sd, size=11, family=MONO, fill=cc)
        ty += 21

    rxx = cxx + chw + 20
    rw = W - 48 - obw - 20 - chw - 20
    panel(rxx, cy, rw, 300, "Place Order",
          "placeGtc / placeIoc / placeFokBudget", GREEN)
    bw = (rw - 36) / 2
    rect(rxx + 12, cy + 46, bw, 34, fill=GREEN_D, stroke=GREEN, rx=6)
    text(rxx + 12 + bw / 2, cy + 68, "BUY / BID", size=12, weight=700, fill=GREEN, anchor="middle")
    rect(rxx + 20 + bw, cy + 46, bw, 34, fill=RED_D, stroke=RED, rx=6)
    text(rxx + 20 + bw + bw / 2, cy + 68, "SELL / ASK", size=12, weight=700, fill=RED, anchor="middle")
    oty = cy + 92
    text(rxx + 14, oty + 12, "Order type", size=10, fill=MUTED)
    for i, t_ in enumerate(["GTC", "IOC", "FOK-BUDGET"]):
        ax = rxx + 12 + i * 116
        on = t_ == "GTC"
        rect(ax, oty + 20, 108, 24, fill=CARD if on else "#161c23",
             stroke=CYAN if on else LINE, rx=5)
        text(ax + 54, oty + 36, t_, size=10, weight=650,
             fill=TXT if on else MUTED, anchor="middle")
    fy = oty + 60
    for name, val in [("Price", "68411.0"), ("Size", "0.50"),
                      ("Reserve limit (bid)", "68414.0"), ("Est. taker fee", "0.0125 USDT")]:
        text(rxx + 14, fy + 12, name, size=10, fill=MUTED)
        rect(rxx + 12, fy + 18, rw - 24, 26, fill="#151b23", stroke=LINE, rx=5)
        text(rxx + 22, fy + 36, val, size=12, fill=TXT, family=MONO)
        fy += 46
    rect(rxx + 12, fy + 2, rw - 24, 38, fill=GREEN, rx=7)
    text(rxx + rw / 2, fy + 27, "PLACE GTC ORDER", size=12.5, weight=800,
         fill="#05160d", anchor="middle")

    oy2 = cy + 320
    panel(rxx, oy2, rw, ch - bfh - 320, "Open Orders",
          "readClient.activeOrders(uid) · cancel / move / reduce", CYAN)
    vx, vy = rxx + 12, oy2 + 46
    vw = rw - 24
    vc = [vw * 0.22, vw * 0.18, vw * 0.30, vw * 0.30]
    thead(vx, vy + 6, ["ID", "SIDE", "PRICE", "SIZE"], vc, ["start", "start", "end", "end"])
    vy += 22
    for oid, sd, p, s in [("8812", "BID", "68411.0", "0.50"),
                          ("8813", "ASK", "68413.0", "2.60"),
                          ("8814", "BID", "68410.5", "1.05")]:
        cc = GREEN if sd == "BID" else RED
        bg = GREEN_D if sd == "BID" else RED_D
        rect(vx, vy, vw, 19, fill=bg, rx=2)
        trow(vx, vy, 19, vc, [oid, "", p, s], ["start", "start", "end", "end"])
        text(vx + vc[0] + 2, vy + 14, sd, size=11, family=MONO, fill=cc)
        vy += 21

    footnote(cx, cy + ch - bfh + 8, W - 48, [
        "L2 order-book depth + spread <- readClient.orderBook(symbolId, maxLevels)",
        "Price chart / market tape <- readClient.marketTrades(symbolId, limit)",
        "Place order (GTC / IOC / FOK-BUDGET) <- placeGtc / placeIoc / placeFokBudget",
        "Open orders <- readClient.activeOrders(uid); cancel / move / reduce <- cancelOrder / moveOrder / reduceOrder"])


def draw_candles(x, y, w, h):
    rect(x, y, w, h, fill=BG, stroke=LINE, rx=5)
    for i in range(6):
        yy = y + h * i / 5
        ELEMS.append(f'<line x1="{x}" y1="{yy:.0f}" x2="{x + w}" y2="{yy:.0f}" stroke="#1e2833" stroke-width="1"/>')
    for i, v in enumerate([68418, 68414, 68410, 68406, 68402, 68398]):
        text(x + w - 4, y + h * i / 5 + 12, str(v), size=8.5, fill=DIM, anchor="end", family=MONO)
    rng = random.Random(7)
    n = 28
    gap = w / n
    base = y + h * 0.45
    for i in range(n):
        cx2 = x + gap * i + gap / 2
        up = i % 3 != 0
        bh = rng.uniform(h * 0.06, h * 0.22)
        op = base + rng.uniform(-h * 0.12, h * 0.12)
        cl = op - (bh if up else -bh)
        hi = min(op, cl) - rng.uniform(h * 0.02, h * 0.06)
        lo = max(op, cl) + rng.uniform(h * 0.02, h * 0.06)
        col = GREEN if up else RED
        ELEMS.append(f'<line x1="{cx2:.0f}" y1="{hi:.0f}" x2="{cx2:.0f}" y2="{lo:.0f}" stroke="{col}" stroke-width="1.5" opacity="0.85"/>')
        by = min(op, cl)
        ELEMS.append(f'<rect x="{cx2 - 4:.0f}" y="{by:.0f}" width="8" height="{max(abs(op - cl), 2):.0f}" fill="{col}" opacity="0.9"/>')
    text(x + 8, y + 14, "68411.0", size=10, weight=700, fill=CYAN, family=MONO)


def screen_account(ox, oy, W, H):
    titlebar(ox + 6, oy + 6, W - 12, 54, "Account & Portfolio  ·  User 10042",
             [("Markets", False), ("Spot", False), ("Order", False),
              ("Portfolio", True), ("Admin", False), ("Ops", False)],
             "REPLICA CAUGHT-UP · 0 ms stale", CYAN_D)
    cx, cy = ox + 24, oy + 74
    cw, ch = W - 48, H - 74 - 64
    bfh = 64
    chipw = (cw - 36) / 4
    chips = [("Total Equity (USDT)", "14,208.44", TXT, None),
             ("In Orders (reserved)", "1,892.40", AMBER, None),
             ("Realized P&L", "+312.60", GREEN, None),
             ("Collected Fees (uid 0)", "41.08", PURPLE, None)]
    for i, (lab, val, f, sub) in enumerate(chips):
        chip(cx + i * (chipw + 12), cy, chipw, 62, lab, val, f, CARD, sub)

    by = cy + 76
    bh = ch - 76 - bfh
    bw = 520
    panel(cx, by, bw, bh, "Balances",
          "readClient.balance(uid, currency) · singleUserReport(uid)", GREEN)
    tx, ty = cx + 14, by + 46
    tw = bw - 28
    tc = [tw * 0.16, tw * 0.28, tw * 0.28, tw * 0.28]
    thead(tx, ty + 6, ["CUR", "AVAILABLE", "RESERVED", "TOTAL"], tc, ["start", "end", "end", "end"])
    ty += 22
    for r in [("USDT", "12,316.04", "1,892.40", "14,208.44"),
              ("BTC", "0.18420", "0.02500", "0.20920"),
              ("ETH", "2.4100", "0.0000", "2.4100")]:
        trow(tx, ty, 22, tc, r, ["start", "end", "end", "end"])
        ty += 24
    rect(tx, ty + 6, tw, 80, fill=CARD, stroke=LINE, rx=6)
    text(tx + 12, ty + 28, "FUNDS RESERVED (direct-exchange risk)", size=10, weight=700, fill=AMBER)
    for i, (lab, val) in enumerate([("Bid holds (size x reserve x quoteScale)", "1,724.10 USDT"),
                                    ("Ask holds (size x baseScale)", "0.02500 BTC"),
                                    ("Reserved taker fee", "168.30 USDT")]):
        text(tx + 12, ty + 48 + i * 19, "· " + lab, size=10, fill=MUTED)
        text(tx + tw - 12, ty + 48 + i * 19, val, size=10, fill=TXT, anchor="end", family=MONO)

    hxx = cx + bw + 20
    hw = cw - bw - 20
    top_h = (bh - 16) / 2
    panel(hxx, by, hw, top_h, "Order History",
          "readClient.orderHistory(uid) · lifecycle + per-fill", CYAN)
    tx2, ty2 = hxx + 14, by + 44
    tw2 = hw - 28
    hc = [40, 66, 42, 60, 64, 46, tw2 - 318]
    thead(tx2, ty2 + 6, ["ID", "PAIR", "SIDE", "TYPE", "PRICE", "SIZE", "STATUS"],
          hc, ["start", "start", "start", "start", "end", "end", "start"])
    ty2 += 22
    hist = [("8812", "BTC/USDT", "BID", "GTC", "68411.0", "0.50", "ACTIVE"),
            ("8701", "BTC/USDT", "ASK", "IOC", "68410.0", "1.20", "COMPLETED"),
            ("8554", "ETH/USDT", "BID", "GTC", "3124.0", "2.00", "CANCELLED"),
            ("8420", "BTC/USDT", "ASK", "FOK", "68420.0", "0.30", "REJECTED")]
    stc = {"ACTIVE": CYAN, "COMPLETED": GREEN, "CANCELLED": MUTED, "REJECTED": RED}
    for oid, pair, sd, tp, p, s, st in hist:
        cc = GREEN if sd == "BID" else RED
        trow(tx2, ty2, 20, hc, [oid, pair, "", tp, p, s, ""],
             ["start", "start", "start", "start", "end", "end", "start"])
        text(tx2 + hc[0] + hc[1] + 2, ty2 + 14, sd, size=10.5, family=MONO, fill=cc)
        text(tx2 + hc[0] + hc[1] + hc[2] + hc[3] + hc[4] + hc[5] + 2, ty2 + 14,
             st, size=10.5, family=MONO, fill=stc[st])
        ty2 += 21

    jy = by + top_h + 16
    panel(hxx, jy, hw, top_h, "My Trades",
          "readClient.userTrades(uid, limit) · fills with counterparty", PURPLE)
    tx3, ty3 = hxx + 14, jy + 44
    tw3 = hw - 28
    mc = [tw3 * 0.13, tw3 * 0.18, tw3 * 0.09, tw3 * 0.24, tw3 * 0.19, tw3 * 0.17]
    thead(tx3, ty3 + 6, ["TIME", "PAIR", "SIDE", "PRICE", "SIZE", "FEE"],
          mc, ["start", "start", "start", "end", "end", "end"])
    ty3 += 22
    for t_, pair, sd, p, s, fee in [("10:41:02", "BTC/USDT", "BUY", "68411.0", "0.25", "0.34"),
                                    ("10:41:01", "BTC/USDT", "SELL", "68412.5", "0.80", "1.02"),
                                    ("10:40:58", "ETH/USDT", "BUY", "3124.0", "1.50", "0.94")]:
        cc = GREEN if sd == "BUY" else RED
        trow(tx3, ty3, 21, mc, [t_, pair, "", p, s, fee],
             ["start", "start", "start", "end", "end", "end"])
        text(tx3 + mc[0] + mc[1] + 2, ty3 + 15, sd, size=10.5, family=MONO, fill=cc)
        ty3 += 22

    footnote(cx, cy + ch - bfh + 8, cw, [
        "Balances (available / reserved) <- readClient.balance + direct-exchange risk hold rules",
        "Reservation breakdown <- DirectExchangeRisk: size x reserveBidPrice x quoteScaleK + takerFee",
        "Order lifecycle + status <- readClient.orderHistory(uid) / order(orderId)",
        "User fills + fees <- readClient.userTrades(uid, limit); collected fees accrue on account 0"])


def screen_admin(ox, oy, W, H):
    titlebar(ox + 6, oy + 6, W - 12, 54, "Operator Console  ·  Exchange Administration",
             [("Markets", False), ("Spot", False), ("Order", False),
              ("Portfolio", False), ("Admin", True), ("Ops", False)],
             "ON LEADER · egress correlated", PURPLE_D)
    cx, cy = ox + 24, oy + 74
    cw, ch = W - 48, H - 74 - 64
    bfh = 64

    sw = 520
    panel(cx, cy, sw, ch - bfh, "Symbols & Fees",
          "addSymbol(base, quote, baseScaleK, quoteScaleK, makerFee, takerFee)", PURPLE)
    tx, ty = cx + 14, cy + 46
    tw = sw - 28
    sc = [tw * 0.22, tw * 0.16, tw * 0.16, tw * 0.23, tw * 0.23]
    thead(tx, ty + 6, ["SYMBOL", "BASE", "QUOTE", "MAKER", "TAKER"], sc,
          ["start", "start", "start", "end", "end"])
    ty += 22
    for r in [("BTC/USDT", "BTC", "USDT", "0.01%", "0.05%"),
              ("ETH/USDT", "ETH", "USDT", "0.02%", "0.06%"),
              ("SOL/USDT", "SOL", "USDT", "0.02%", "0.10%")]:
        trow(tx, ty, 20, sc, r, ["start", "start", "start", "end", "end"])
        ty += 21
    rect(tx, ty + 8, tw, 100, fill=CARD, stroke=LINE, rx=6)
    text(tx + 12, ty + 28, "ADD SYMBOL (schema v2 fees)", size=10, weight=700, fill=PURPLE)
    pr = [("baseScaleK", "1e8"), ("quoteScaleK", "1e6"), ("makerFee", "0.0100%"), ("takerFee", "0.0500%")]
    for i, (f, v) in enumerate(pr):
        fw = (tw - 24) / 2
        xr = tx + 12 + (i % 2) * (fw + 8)
        yr = ty + 38 + (i // 2) * 27
        rect(xr, yr, fw, 21, fill="#151b23", stroke=LINE, rx=4)
        text(xr + 6, yr + 15, f, size=9, fill=MUTED)
        text(xr + fw - 6, yr + 15, v, size=9.5, fill=TXT, anchor="end", family=MONO)
    rect(tx, ty + 118, tw, 30, fill=PURPLE, rx=7)
    text(tx + tw / 2, ty + 138, "ADD SYMBOL", size=11.5, weight=800, fill="#160f24", anchor="middle")

    uxx = cx + sw + 20
    uw = cw - sw - 20
    top_h = (ch - 16 - bfh) * 0.55
    panel(uxx, cy, uw, top_h, "Users & Status",
          "addUser(uid) · suspendUser / resumeUser(uid)", CYAN)
    tx2, ty2 = uxx + 14, cy + 44
    tw2 = uw - 28
    uc = [tw2 * 0.16, tw2 * 0.24, tw2 * 0.24, tw2 * 0.22]
    thead(tx2, ty2 + 6, ["UID", "BALANCE USDT", "BALANCE BTC", "STATUS"], uc,
          ["start", "end", "end", "start"])
    ty2 += 22
    for u, b1, b2, st in [("10042", "14,208.44", "0.20920", "ACTIVE"),
                          ("10043", "0.00", "0.00000", "ACTIVE"),
                          ("10044", "3,010.80", "0.00000", "SUSPENDED"),
                          ("0", "41.08", "0.00000", "FEE ACCT")]:
        sc_ = AMBER if st == "SUSPENDED" else PURPLE if st == "FEE ACCT" else GREEN
        trow(tx2, ty2, 20, uc, [u, b1, b2, ""], ["start", "end", "end", "start"])
        text(tx2 + uc[0] + uc[1] + uc[2] + 2, ty2 + 14, st, size=10.5, family=MONO, fill=sc_)
        ty2 += 21
    rect(tx2, ty2 + 8, tw2, 66, fill=CARD, stroke=LINE, rx=6)
    text(tx2 + 12, ty2 + 26, "ACCOUNT ACTIONS", size=9.5, weight=700, fill=CYAN)
    acts = [("ADD USER", CYAN, "#04202e"), ("ADJ BAL", AMBER, "#2a2410"),
            ("SUSPEND", RED, "#2a1416"), ("RESUME", GREEN, "#0f1f14")]
    for i, (a, bg, fc) in enumerate(acts):
        aw = (tw2 - 36) / 4
        ax = tx2 + 12 + i * (aw + 8)
        rect(ax, ty2 + 34, aw, 24, fill=fc, stroke=a, rx=5)
        text(ax + aw / 2, ty2 + 50, a.upper(), size=9, weight=700, anchor="middle")

    y3 = cy + 46 + top_h + 8
    b3 = (ch - 16 - bfh) * 0.45 - 8
    panel(uxx, y3, uw, b3, "Risk & Value Conservation",
          "report.totalCurrencyBalance() · maker/taker fee math", GREEN)
    tx3, ty3 = uxx + 14, y3 + 46
    tw3 = uw - 28
    for i, (lab, val) in enumerate([("Client account balances", "12,316.04 USDT + 0.18420 BTC"),
                                    ("Collected fees (account 0)", "41.08 USDT"),
                                    ("Reserved order balances", "1,892.40 USDT"),
                                    ("Conservation invariant", "taker + maker + fee = constant")]):
        if i == 3:
            rect(tx3, ty3 + 3 * 26, tw3, 30, fill="#102433", stroke=LINE, rx=6)
            text(tx3 + 12, ty3 + 3 * 26 + 19, "· " + lab, size=10.5, fill=CYAN)
        else:
            text(tx3 + 12, ty3 + i * 26 + 14, "· " + lab, size=10.5, fill=MUTED)
            text(tx3 + tw3 - 12, ty3 + i * 26 + 14, val, size=10.5, fill=TXT, anchor="end", family=MONO)

    footnote(cx, cy + ch - bfh + 8, cw, [
        "Add symbol + scales + maker / taker fees <- addSymbol (ADD_SYMBOL), validated by SymbolSpecStore",
        "Add user / balance adjustment <- addUser / adjustBalance (BALANCE_ADJUSTMENT)",
        "Suspend / resume order placement <- suspendUser / resumeUser; blocked -> USER_SUSPENDED",
        "Value conservation (taker + maker + fee constant) <- report.totalCurrencyBalance() / stateHash()"])


def screen_ops(ox, oy, W, H):
    titlebar(ox + 6, oy + 6, W - 12, 54, "System Health  ·  Determinism & Replication",
             [("Markets", False), ("Spot", False), ("Order", False),
              ("Portfolio", False), ("Admin", False), ("Ops", True)],
             "CLUSTER QUORUM OK · replica caught-up", RED_D)
    cx, cy = ox + 24, oy + 74
    cw, ch = W - 48, H - 74 - 64
    bfh = 64

    tw_ = 400
    panel(cx, cy, tw_, ch - bfh, "Cluster Topology",
          "3-node Raft + CQRS replica + journal", CYAN)
    nx, ny = cx + 22, cy + 58
    nw = tw_ - 44
    for name, role, c in [("Node-0", "LEADER", GREEN),
                          ("Node-1", "FOLLOWER", MUTED),
                          ("Node-2", "FOLLOWER", MUTED)]:
        rect(nx, ny, nw, 64, fill=CARD, stroke=LINE, rx=8)
        rect(nx, ny, 4, 64, fill=c, rx=2)
        text(nx + 14, ny + 25, name, size=13, weight=700)
        text(nx + 14, ny + 46, "Raft · archive log · snapshot", size=9.5, fill=MUTED)
        text(nx + nw - 12, ny + 25, role, size=10, weight=800, fill=c, anchor="end")
        text(nx + nw - 12, ny + 46, "applied 1,204,118", size=9.5, fill=DIM, anchor="end", family=MONO)
        ny += 76
    for name, role, c, sub in [("Read Replica", "CAUGHT-UP", CYAN, "isCaughtUp() · appliedPosition"),
                               ("Domain Journal", "EXACTLY-ONCE", PURPLE, "(logPosition, eventIndex) dedup"),
                               ("Write Client", "IDEMPOTENT", AMBER, "clientId + clientSeq retry")]:
        rect(nx, ny, nw, 60, fill=CARD, stroke=LINE, rx=8)
        rect(nx, ny, 4, 60, fill=c, rx=2)
        text(nx + 14, ny + 23, name, size=12.5, weight=700)
        text(nx + 14, ny + 44, sub, size=9.5, fill=MUTED)
        text(nx + nw - 12, ny + 23, role, size=9.5, weight=800, fill=c, anchor="end")
        ny += 70

    mxx = cx + tw_ + 20
    mw = 560
    panel(mxx, cy, mw, ch - bfh, "Telemetry & Tail Latency",
          "CoreMetrics counters · off-heap mirror (AtomicCounterSink)", AMBER)
    mx, my = mxx + 16, cy + 52
    mtw = mw - 32
    counters = [("commands processed", "12,004,118"), ("duplicates (dedup hits)", "1,881"),
                ("backpressure stalls", "3"), ("unsupported commands", "0"),
                ("snapshots taken / loaded", "214 / 212"), ("event-buffer overflows", "0"),
                ("order-pool exhaustions", "0"), ("price-bucket exhaustions", "0"),
                ("journal backpressure", "7"), ("journal recorder errors", "0")]
    rw_ = mtw / 2 - 6
    for i, (k, v) in enumerate(counters):
        rx_ = mx + (i % 2) * (rw_ + 12)
        ry_ = my + (i // 2) * 24
        text(rx_, ry_ + 13, "· " + k, size=10, fill=MUTED)
        text(rx_ + rw_, ry_ + 13, v, size=11, fill=TXT, anchor="end", family=MONO)
    ly = my + ((len(counters) + 1) // 2) * 24 + 8
    rect(mx, ly, mtw, 96, fill=CARD, stroke=LINE, rx=6)
    text(mx + 12, ly + 22, "TAIL LATENCY (end-to-end IPC, HdrHistogram)", size=9.5, weight=700, fill=AMBER)
    bars = [("p50", "6.2 us", 0.18, GREEN), ("p99", "18.4 us", 0.42, CYAN),
            ("p99.9", "31.0 us", 0.62, AMBER), ("p99.99", "46.1 us", 0.96, RED)]
    seg = (mtw - 24) / 4
    for i, (name, val, frac, cc) in enumerate(bars):
        bx_ = mx + 12 + i * seg
        bw_ = seg - 10
        rect(bx_, ly + 32, bw_, 8, fill="#1e2833", rx=4)
        rect(bx_, ly + 32, bw_ * frac, 8, fill=cc, rx=4)
        text(bx_, ly + 56, name, size=9.5, fill=MUTED)
        text(bx_, ly + 72, val, size=10, weight=700, family=MONO)

    rxx = mxx + mw + 20
    rw = cw - tw_ - 20 - mw - 20
    rh = (ch - 16 - bfh) * 0.5
    panel(rxx, cy, rw, rh, "Replica Health",
          "ReplicationHealth · appliedPosition", CYAN)
    rx2, ry2 = rxx + 14, cy + 50
    rtw = rw - 28
    for i, (lab, val) in enumerate([("applied (cluster-global)", "1,204,118"),
                                    ("active archive endpoint", "node-0:20104"),
                                    ("failover switches", "2"),
                                    ("snapshot bootstrap", "yes"),
                                    ("caught up", "true")]):
        rect(rx2, ry2 + i * 25, rtw, 20, fill="#171d24", stroke=LINE, rx=4)
        text(rx2 + 10, ry2 + i * 25 + 14, lab, size=10, fill=MUTED)
        text(rx2 + rtw - 10, ry2 + i * 25 + 14, val, size=11, fill=TXT, anchor="end", family=MONO)
    text(rx2, ry2 + 5 * 25 + 4, "STATE HASH (deterministic fingerprint)", size=9.5, weight=700, fill=AMBER)
    rect(rx2, ry2 + 5 * 25 + 14, rtw, 26, fill="#102433", stroke=CYAN, rx=5)
    text(rx2 + 10, ry2 + 5 * 25 + 32, "0x9f3c...a17b", size=11, fill=CYAN, family=MONO)

    jy = cy + rh + 8
    jh = ch - 16 - bfh - rh
    panel(rxx, jy, rw, jh, "Audit Journal / Events",
          "JournalEvent stream 200 · exactly-once", PURPLE)
    jx, jy2 = rxx + 14, jy + 46
    jtw = rw - 28
    for k, pos, typ, c in [("T", "logPos 8402112 · ev 0", "TRADE", GREEN),
                           ("R", "logPos 8402112 · ev 1", "REDUCE", CYAN),
                           ("T", "logPos 8402115 · ev 0", "TRADE", GREEN),
                           ("J", "logPos 8402120 · ev 0", "REJECT", RED)]:
        rect(jx, jy2, jtw, 26, fill="#171d24", stroke=LINE, rx=5)
        ELEMS.append(f'<circle cx="{jx + 14}" cy="{jy2 + 13}" r="4" fill="{c}"/>')
        text(jx + 28, jy2 + 17, k, size=9, weight=700, fill=c)
        text(jx + 52, jy2 + 17, pos, size=10, fill=MUTED, family=MONO)
        text(jx + jtw - 10, jy2 + 17, typ, size=10, weight=700, fill=c, anchor="end")
        jy2 += 30

    footnote(cx, cy + ch - bfh + 8, cw, [
        "Cluster role / applied position <- ReplicationHealth + isCaughtUp(); positions are cluster-global",
        "Telemetry counters + latency tails <- CoreMetrics (off-heap AtomicCounterSink), 0-alloc hot path",
        "Value conservation + state hash <- report.totalCurrencyBalance() / report.stateHash()",
        "Audit events <- JournalEvent (logPosition, eventIndex) on stream 200; deduped exactly-once"])


def board_frame(x, y, w, h):
    rect(x + 6, y + 6, w, h, fill="#0a0d11", rx=10, opacity=0.5)
    rect(x, y, w, h, fill=BOARD, stroke=LINE, rx=10, sw=1.2)


def build():
    W, H = 3060, 1880
    rect(0, 0, W, H, fill=BG)
    text(30, 42, "excoredum  ·  Spot Matching Engine - UI Design Reference", size=24, weight=800)
    text(30, 64, "Dark-theme mockup for building a UI on top of the deterministic CQRS matching engine. "
                 "Each region maps to an engine command or read-side query.", size=12, fill=MUTED)

    board_w, board_h = 1450, 730
    col_x = [30, 30 + board_w + 40]
    row_y = [96, 96 + board_h + 40]
    labels = [("A", "Trading Terminal"), ("B", "Account & Portfolio"),
              ("C", "Operator Console"), ("D", "System Health & Ops")]
    screens = [screen_trading, screen_account, screen_admin, screen_ops]
    for i, fn in enumerate(screens):
        cx = col_x[i % 2]
        cy = row_y[i // 2]
        board_frame(cx, cy, board_w, board_h)
        text(cx + 4, cy - 10, f"SCREEN {labels[i][0]}  ·  {labels[i][1]}", size=12, weight=700, fill=CYAN)
        fn(cx, cy, board_w, board_h)

    ly = row_y[1] + board_h + 20
    text(30, ly + 14, "LEGEND", size=11, weight=800, fill=MUTED)
    for name, c in [("Bid / Buy", GREEN), ("Ask / Sell", RED),
                    ("Admin / Risk", PURPLE), ("Market / Info", CYAN),
                    ("Warn / Pending", AMBER)]:
        rect(90 + 172 * list(["Bid / Buy", "Ask / Sell", "Admin / Risk", "Market / Info", "Warn / Pending"]).index(name), ly, 14, 14, fill=c, rx=3)
        text(90 + 172 * list(["Bid / Buy", "Ask / Sell", "Admin / Risk", "Market / Info", "Warn / Pending"]).index(name) + 22, ly + 12, name, size=11)

    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img">' % (W, H, W, H))
    return svg + "\n".join(ELEMS) + "\n</svg>"


if __name__ == "__main__":
    out = "../excoredum-ui-mockup.svg"
    with open(out, "w", encoding="utf-8") as f:
        f.write(build())
    print("written:", out)
