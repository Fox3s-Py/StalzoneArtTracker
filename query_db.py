import sqlite3

# 1. Find lots with high target_price / fair_value
conn = sqlite3.connect('auction_scores.sqlite3')
conn.row_factory = sqlite3.Row
print("=== Lots with target_price > 2000000 ===")
rows = conn.execute(
    'SELECT ls.lot_key, ls.item_id, ls.qlt, ls.ptn, ls.buyout_price, '
    'ls.fair_value, ls.target_price, ls.absolute_profit, ls.percent_profit, '
    'ls.pass_filter, ls.reject_reason, fv.segmentation, fv.sale_count, fv.blend_weight '
    'FROM lot_scores ls JOIN item_fair_value fv USING (item_id, qlt, ptn) '
    'WHERE ls.target_price > 2000000 ORDER BY ls.target_price DESC LIMIT 20'
).fetchall()
for r in rows:
    print(dict(r))
conn.close()

# 2. Check the item_fair_value table for high fair values
conn = sqlite3.connect('auction_scores.sqlite3')
conn.row_factory = sqlite3.Row
print("\n=== item_fair_value with fair_value > 2000000 ===")
rows = conn.execute(
    'SELECT item_id, qlt, ptn, fair_value, std_dev, sale_count, lambda_per_day, '
    'confidence, segmentation, low_confidence, market_cooling, blend_weight '
    'FROM item_fair_value WHERE fair_value > 2000000 ORDER BY fair_value DESC LIMIT 20'
).fetchall()
for r in rows:
    print(dict(r))
conn.close()

# 3. Check history for a specific item - let's find items with ptn=4
conn = sqlite3.connect('auction_history.sqlite3')
conn.row_factory = sqlite3.Row
print("\n=== Tables in history DB ===")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'hist_%'").fetchall()
print(f"Total tables: {len(tables)}")

# Find tables that have ptn=4 sales
print("\n=== Tables with ptn=4 sales ===")
for t in tables:
    table_name = t[0]
    try:
        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE ptn = 4').fetchone()[0]
        if count > 0:
            print(f"  {table_name}: {count} sales with ptn=4")
    except:
        pass
conn.close()
