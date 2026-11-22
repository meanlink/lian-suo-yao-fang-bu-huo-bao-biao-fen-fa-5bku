import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from mysql.connector import IntegrityError
from pydantic import BaseModel, ConfigDict, Field

from .auth import current_user, require
from .db import conn, query

router = APIRouter()
MANAGER = require('regional_manager')
INVENTORY = require('inventory_clerk', 'regional_manager')
SHIFT = require('shift_coordinator', 'regional_manager')
LOCAL_ZONE = ZoneInfo('Asia/Shanghai')
LATEST = '''NOT EXISTS (SELECT 1 FROM inventory_snapshots newer WHERE newer.store_code=s.store_code
            AND newer.sku=s.sku AND newer.snapshot_date>s.snapshot_date)'''
RECEIPT_GAP = '''d.status='sent' AND (d.dispatched_at IS NULL OR d.dispatched_at <= NOW() - INTERVAL 2 HOUR)'''


class Note(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    note: str = Field(min_length=3, max_length=255)


class ExpiryAction(Note):
    action: Literal['quarantined', 'returned']


class Subscription(BaseModel):
    store_code: str = Field(min_length=1, max_length=40)
    recipient_id: int = Field(gt=0)
    delivery_hour: int = Field(ge=0, le=23)


class Enabled(BaseModel):
    enabled: bool


class ApprovalLink(BaseModel):
    shortage_id: int | None = Field(default=None, gt=0)


class Review(BaseModel):
    decision: Literal['approved', 'rejected']
    note: str = ''
    shortage_id: int | None = Field(default=None, gt=0)


def _column_exists(table, column):
    rows, _ = query('''SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()
                       AND TABLE_NAME=%s AND COLUMN_NAME=%s''', (table, column))
    return bool(rows)


def _index_exists(table, index):
    rows, _ = query('''SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE()
                       AND TABLE_NAME=%s AND INDEX_NAME=%s''', (table, index))
    return bool(rows)


def _migrate():
    """Bring databases created before the shortage rework up to the current shape."""
    if not _column_exists('shortage_cases', 'store_code'):
        query('ALTER TABLE shortage_cases ADD COLUMN store_code VARCHAR(40) NULL', fetch=False)
    if not _column_exists('shortage_cases', 'sku'):
        query('ALTER TABLE shortage_cases ADD COLUMN sku VARCHAR(40) NULL', fetch=False)
    if not _column_exists('shortage_cases', 'shortage_qty'):
        query('ALTER TABLE shortage_cases ADD COLUMN shortage_qty DECIMAL(12,2) NULL', fetch=False)
    if not _column_exists('shortage_cases', 'open_slot'):
        query('''UPDATE shortage_cases c JOIN inventory_snapshots s ON s.id=c.snapshot_id
                 SET c.store_code=COALESCE(c.store_code,s.store_code),c.sku=COALESCE(c.sku,s.sku)''', fetch=False)
        query('''UPDATE shortage_cases c JOIN inventory_snapshots s ON s.id=c.snapshot_id
                 LEFT JOIN (SELECT snapshot_id,MAX(id) latest_id FROM replenishment_recommendations
                            GROUP BY snapshot_id) m ON m.snapshot_id=s.id
                 LEFT JOIN replenishment_recommendations r ON r.id=m.latest_id
                 SET c.shortage_qty=COALESCE(c.shortage_qty,GREATEST(0,COALESCE(r.reorder_point,s.on_hand)-s.on_hand))''',
              fetch=False)
        # Historical duplicates came from re-counting snapshots; keep the earliest open case.
        query('''UPDATE shortage_cases c JOIN (SELECT store_code,sku,MIN(snapshot_id) keep_id FROM shortage_cases
                 WHERE status='open' GROUP BY store_code,sku HAVING COUNT(*)>1) d
                 ON d.store_code=c.store_code AND d.sku=c.sku AND c.status='open' AND c.snapshot_id<>d.keep_id
                 SET c.status='resolved',c.resolution=CONCAT('合并到缺货单快照 #',d.keep_id),c.resolved_at=NOW()''', fetch=False)
        query('''UPDATE shortage_cases SET store_code='' WHERE store_code IS NULL''', fetch=False)
        query('''UPDATE shortage_cases SET sku=CONCAT('#',snapshot_id) WHERE sku IS NULL''', fetch=False)
        query('''UPDATE shortage_cases SET shortage_qty=0 WHERE shortage_qty IS NULL''', fetch=False)
        query('''ALTER TABLE shortage_cases MODIFY store_code VARCHAR(40) NOT NULL,
                 MODIFY sku VARCHAR(40) NOT NULL, MODIFY shortage_qty DECIMAL(12,2) NOT NULL''', fetch=False)
        query('''ALTER TABLE shortage_cases ADD COLUMN open_slot TINYINT
                 GENERATED ALWAYS AS (IF(status='open',1,NULL)) STORED''', fetch=False)
    if not _index_exists('shortage_cases', 'uq_shortage_open'):
        query('ALTER TABLE shortage_cases ADD UNIQUE KEY uq_shortage_open (store_code,sku,open_slot)', fetch=False)
    if not _column_exists('approval_orders', 'shortage_id'):
        query('ALTER TABLE approval_orders ADD COLUMN shortage_id INT NULL', fetch=False)
    if not _index_exists('approval_orders', 'ix_approval_shortage'):
        query('ALTER TABLE approval_orders ADD KEY ix_approval_shortage (shortage_id)', fetch=False)


def initialize():
    schema = Path(__file__).resolve().parents[1] / 'operations.sql'
    for statement in schema.read_text().split(';'):
        if statement.strip():
            query(statement, fetch=False)
    _migrate()


@contextmanager
def operational_transaction():
    db = conn()
    cur = db.cursor(dictionary=True, buffered=True)
    try:
        yield cur
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        cur.close()
        db.close()


def audit(cur, actor, event, kind, entity_id, **detail):
    cur.execute('''INSERT INTO audit_logs(actor_id,action,entity_type,entity_id,detail) VALUES(%s,%s,%s,%s,%s)''',
                (actor, event, kind, entity_id, json.dumps(detail, ensure_ascii=False)))


@router.get('/expiry-board')
def expiry_board(_user=Depends(current_user)):
    cutoff = datetime.now(LOCAL_ZONE).date() + timedelta(days=30)
    rows, _ = query(f'''SELECT s.id,s.store_code,s.product_name,s.expiry_date,s.on_hand,s.substitute_name,
                       a.action,a.note FROM inventory_snapshots s LEFT JOIN expiry_actions a ON a.snapshot_id=s.id
                       WHERE {LATEST} AND s.expiry_date<=%s AND s.on_hand>0 ORDER BY s.expiry_date,s.id''', (cutoff,))
    return rows


@router.post('/expiry-board/{snapshot_id}', status_code=201)
def expiry_action(snapshot_id: int, data: ExpiryAction, user=Depends(INVENTORY)):
    cutoff = datetime.now(LOCAL_ZONE).date() + timedelta(days=30)
    with operational_transaction() as cur:
        cur.execute(f'SELECT s.id FROM inventory_snapshots s WHERE s.id=%s AND {LATEST} AND s.expiry_date<=%s AND s.on_hand>0 FOR UPDATE',
                    (snapshot_id, cutoff))
        if not cur.fetchone():
            raise HTTPException(409, '仅可处理最新在库且 30 天内到期或已过期的盘点')
        try:
            cur.execute('INSERT INTO expiry_actions(snapshot_id,action,note,actor_id) VALUES(%s,%s,%s,%s)',
                        (snapshot_id, data.action, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该临期快照已有处置记录') from error
        audit(cur, user['sub'], 'expiry_action', 'inventory_snapshot', snapshot_id, action=data.action, note=data.note)
    return {'action': data.action}


def _shortage_receipts(cur, shortage_id):
    cur.execute('''SELECT d.id dispatch_id,d.approval_id,d.status,d.recipient_name,d.shift,
                   d.dispatched_at,d.received_at,a.approved_qty
                   FROM approval_orders a JOIN dispatch_records d ON d.approval_id=a.id
                   WHERE a.shortage_id=%s ORDER BY d.id''', (shortage_id,))
    return cur.fetchall()


def lock_shortage_link(cur, shortage_id, store_code, sku):
    """Only the open case for the same store and SKU may be credited by another transfer order."""
    cur.execute('''SELECT snapshot_id,store_code,sku,shortage_qty,status FROM shortage_cases
                   WHERE snapshot_id=%s FOR UPDATE''', (shortage_id,))
    case = cur.fetchone()
    if not case:
        raise HTTPException(409, '缺货升级记录不存在')
    if case['status'] != 'open':
        raise HTTPException(409, '该缺货升级已结案，不能再关联调货单')
    if case['store_code'] != store_code or case['sku'] != sku:
        raise HTTPException(409, '只能关联同一门店同一药品的缺货升级')
    return case


@router.get('/shortage-board')
def shortages(_user=Depends(current_user)):
    # One open case per store+SKU survives new count sheets; the latest snapshot is the row identity.
    rows, _ = query(f'''SELECT s.id,s.store_code,s.sku,s.product_name,s.on_hand,r.reorder_point,
                       c.snapshot_id case_snapshot_id,c.shortage_qty,c.status,c.note,c.resolution,
                       c.opened_at,c.resolved_at,
                       COALESCE((SELECT SUM(a.approved_qty) FROM approval_orders a
                                 JOIN dispatch_records d ON d.approval_id=a.id AND d.status='received'
                                 WHERE a.shortage_id=c.snapshot_id),0) received_qty
                       FROM inventory_snapshots s
                       LEFT JOIN replenishment_recommendations r ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                       LEFT JOIN shortage_cases c ON c.store_code=s.store_code AND c.sku=s.sku
                            AND c.status='open'
                       WHERE {LATEST} AND (c.snapshot_id IS NOT NULL OR (r.id IS NOT NULL AND s.on_hand<r.reorder_point))
                       ORDER BY c.status='open' DESC,s.store_code,s.sku''')
    history, _ = query(f'''SELECT NULL id,c.store_code,c.sku,s.product_name,s.on_hand,r.reorder_point,
                          c.snapshot_id case_snapshot_id,c.shortage_qty,c.status,c.note,c.resolution,
                          c.opened_at,c.resolved_at,
                          COALESCE((SELECT SUM(a.approved_qty) FROM approval_orders a
                                    JOIN dispatch_records d ON d.approval_id=a.id AND d.status='received'
                                    WHERE a.shortage_id=c.snapshot_id),0) received_qty
                          FROM shortage_cases c JOIN inventory_snapshots s ON s.id=c.snapshot_id
                          LEFT JOIN replenishment_recommendations r ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                          WHERE c.status='resolved' ORDER BY c.resolved_at DESC,c.snapshot_id DESC''')
    with operational_transaction() as cur:
        for row in rows + history:
            row['shortage_qty'] = float(row['shortage_qty'] or 0)
            row['received_qty'] = float(row['received_qty'] or 0)
            row['remaining_qty'] = max(0.0, row['shortage_qty'] - row['received_qty'])
            row['receipts'] = _shortage_receipts(cur, row['case_snapshot_id']) if row['case_snapshot_id'] else []
    return rows + history


@router.post('/shortage-board/{snapshot_id}', status_code=201)
def escalate(snapshot_id: int, data: Note, user=Depends(INVENTORY)):
    with operational_transaction() as cur:
        cur.execute(f'''SELECT s.id,s.store_code,s.sku,s.on_hand,r.reorder_point FROM inventory_snapshots s
                     JOIN replenishment_recommendations r
                     ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                     WHERE s.id=%s AND {LATEST} AND s.on_hand<r.reorder_point FOR UPDATE''', (snapshot_id,))
        snapshot = cur.fetchone()
        if not snapshot:
            raise HTTPException(409, '请对库存不足的最新快照先运行预测')
        # An earlier count sheet may still own the open case for this store+SKU; keep following it.
        cur.execute('''SELECT snapshot_id FROM shortage_cases
                       WHERE store_code=%s AND sku=%s AND status='open' FOR UPDATE''',
                    (snapshot['store_code'], snapshot['sku']))
        existing = cur.fetchone()
        if existing:
            raise HTTPException(409, f'该门店该药品已有未结缺货升级（快照 #{existing["snapshot_id"]}），请继续跟进')
        shortage_qty = max(0, float(snapshot['reorder_point']) - float(snapshot['on_hand']))
        try:
            cur.execute('''INSERT INTO shortage_cases(snapshot_id,store_code,sku,shortage_qty,note,opened_by)
                           VALUES(%s,%s,%s,%s,%s,%s)''',
                        (snapshot_id, snapshot['store_code'], snapshot['sku'], shortage_qty, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该门店该药品已有未结缺货升级，请继续跟进原单') from error
        audit(cur, user['sub'], 'shortage_escalated', 'inventory_snapshot', snapshot_id,
              store_code=snapshot['store_code'], sku=snapshot['sku'], shortage_qty=shortage_qty, note=data.note)
    return {'status': 'open', 'shortage_qty': shortage_qty}


@router.put('/shortage-board/{snapshot_id}')
def resolve_shortage(snapshot_id: int, data: Note, user=Depends(MANAGER)):
    with operational_transaction() as cur:
        cur.execute('''SELECT snapshot_id,store_code,sku,shortage_qty FROM shortage_cases
                       WHERE store_code=(SELECT store_code FROM inventory_snapshots WHERE id=%s)
                       AND sku=(SELECT sku FROM inventory_snapshots WHERE id=%s)
                       AND status='open' FOR UPDATE''', (snapshot_id, snapshot_id))
        case = cur.fetchone()
        if not case:
            raise HTTPException(409, '没有待关闭的缺货升级记录')
        # Lock linked receipts so a concurrent signing in/out cannot change the total mid-check.
        cur.execute('''SELECT d.id FROM approval_orders a JOIN dispatch_records d ON d.approval_id=a.id
                       WHERE a.shortage_id=%s FOR UPDATE''', (case['snapshot_id'],))
        cur.execute('''SELECT COALESCE(SUM(a.approved_qty),0) received_qty FROM approval_orders a
                       JOIN dispatch_records d ON d.approval_id=a.id AND d.status='received'
                       WHERE a.shortage_id=%s''', (case['snapshot_id'],))
        received = float(cur.fetchone()['received_qty'])
        if received < float(case['shortage_qty']):
            raise HTTPException(409, f'签收量不足，尚缺 {float(case["shortage_qty"]) - received:g} 件，不能结案')
        cur.execute('''UPDATE shortage_cases SET status='resolved',resolution=%s,resolved_by=%s,resolved_at=NOW()
                       WHERE snapshot_id=%s AND status='open' ''', (data.note, user['sub'], case['snapshot_id']))
        if cur.rowcount != 1:
            raise HTTPException(409, '没有待关闭的缺货升级记录')
        audit(cur, user['sub'], 'shortage_resolved', 'inventory_snapshot', case['snapshot_id'],
              resolution=data.note, shortage_qty=float(case['shortage_qty']), received_qty=received)
    return {'status': 'resolved', 'shortage_qty': float(case['shortage_qty']), 'received_qty': received}


@router.get('/report-subscriptions')
def subscriptions(user=Depends(current_user)):
    manager = user['role'] == 'regional_manager'
    subscriptions, _ = query('''SELECT rp.*,u.name AS recipient FROM report_subscriptions rp JOIN users u ON u.id=rp.recipient_id
                               WHERE %s OR u.id=%s ORDER BY rp.id''', (manager, user['sub']))
    deliveries, _ = query('''SELECT d.id,d.report_date,d.delivered_at,d.payload,rp.store_code,u.name AS recipient
                             FROM report_deliveries d JOIN report_subscriptions rp ON rp.id=d.subscription_id
                             JOIN users u ON u.id=rp.recipient_id WHERE %s OR u.id=%s
                             ORDER BY d.id DESC LIMIT 60''', (manager, user['sub']))
    for row in deliveries:
        row['payload'] = json.loads(row['payload'])
    stores, _ = query('SELECT DISTINCT store_code,store_name FROM inventory_snapshots ORDER BY store_code') if manager else ([], 0)
    recipients, _ = query('SELECT id,name FROM users ORDER BY id') if manager else ([], 0)
    return {'subscriptions': subscriptions, 'deliveries': deliveries, 'stores': stores, 'recipients': recipients}


@router.post('/report-subscriptions', status_code=201)
def create_subscription(data: Subscription, user=Depends(MANAGER)):
    with operational_transaction() as cur:
        cur.execute('SELECT id FROM inventory_snapshots WHERE store_code=%s LIMIT 1', (data.store_code,))
        if not cur.fetchone():
            raise HTTPException(404, '门店不存在')
        cur.execute('SELECT id FROM users WHERE id=%s', (data.recipient_id,))
        if not cur.fetchone():
            raise HTTPException(404, '收件人不存在')
        try:
            cur.execute('''INSERT INTO report_subscriptions(store_code,recipient_id,delivery_hour,created_on)
                           VALUES(%s,%s,%s,%s)''', (data.store_code, data.recipient_id, data.delivery_hour,
                                                  datetime.now(LOCAL_ZONE).date()))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该门店、收件人和时间已有订阅') from error
        identifier = cur.lastrowid
        audit(cur, user['sub'], 'report_subscribed', 'report_subscription', identifier, store=data.store_code)
    return {'id': identifier}


@router.put('/report-subscriptions/{subscription_id}')
def toggle_subscription(subscription_id: int, data: Enabled, user=Depends(MANAGER)):
    with operational_transaction() as cur:
        cur.execute('SELECT id,enabled FROM report_subscriptions WHERE id=%s FOR UPDATE', (subscription_id,))
        current = cur.fetchone()
        if not current:
            raise HTTPException(404, '订阅不存在')
        if bool(current['enabled']) != data.enabled:
            cur.execute('UPDATE report_subscriptions SET enabled=%s WHERE id=%s', (data.enabled, subscription_id))
            audit(cur, user['sub'], 'report_schedule_updated', 'report_subscription', subscription_id, enabled=data.enabled)
    return {'enabled': data.enabled}


def deliver_due_reports(now=None):
    now = now or datetime.now(LOCAL_ZONE)
    with operational_transaction() as cur:
        cur.execute('''SELECT s.* FROM report_subscriptions s WHERE s.enabled AND s.created_on<=%s AND s.delivery_hour<=%s
                       AND NOT EXISTS (SELECT 1 FROM report_deliveries d WHERE d.subscription_id=s.id AND d.report_date=%s)
                       ORDER BY s.id LIMIT 50 FOR UPDATE SKIP LOCKED''', (now.date(), now.hour, now.date()))
        for subscription in cur.fetchall():
            cur.execute('''SELECT s.sku,s.product_name,s.snapshot_date,s.on_hand,r.reorder_point,r.recommended_qty,r.status
                          FROM inventory_snapshots s LEFT JOIN replenishment_recommendations r
                          ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                          WHERE s.store_code=%s AND s.snapshot_date<=%s
                          AND NOT EXISTS (SELECT 1 FROM inventory_snapshots newer WHERE newer.store_code=s.store_code
                          AND newer.sku=s.sku AND newer.snapshot_date>s.snapshot_date AND newer.snapshot_date<=%s) ORDER BY s.sku''',
                        (subscription['store_code'], now.date(), now.date()))
            payload = json.dumps(cur.fetchall(), ensure_ascii=False, default=str)
            cur.execute('INSERT INTO report_deliveries(subscription_id,report_date,payload) VALUES(%s,%s,%s)',
                        (subscription['id'], now.date(), payload))


@router.get('/receipt-audit')
def receipt_audit(_user=Depends(current_user)):
    rows, _ = query(f'''SELECT d.id,d.recipient_name,d.shift,d.status,d.dispatched_at,d.received_at,
                       a.store_code,a.approved_qty,e.reason,e.closed_at,({RECEIPT_GAP}) AS overdue
                       FROM dispatch_records d JOIN approval_orders a ON a.id=d.approval_id
                       LEFT JOIN receipt_exceptions e ON e.dispatch_id=d.id
                       WHERE d.status IN ('sent','received') ORDER BY overdue DESC,d.id DESC LIMIT 120''')
    return rows


@router.post('/receipt-audit/{dispatch_id}', status_code=201)
def flag_receipt(dispatch_id: int, data: Note, user=Depends(SHIFT)):
    with operational_transaction() as cur:
        cur.execute(f'SELECT d.id FROM dispatch_records d WHERE d.id=%s AND {RECEIPT_GAP} FOR UPDATE', (dispatch_id,))
        if not cur.fetchone():
            raise HTTPException(409, '只有超过 2 小时未签收或缺失发送时间的分发可以登记异常')
        try:
            cur.execute('INSERT INTO receipt_exceptions(dispatch_id,reason,opened_by) VALUES(%s,%s,%s)',
                        (dispatch_id, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该分发已有异常记录') from error
        audit(cur, user['sub'], 'receipt_flagged', 'dispatch', dispatch_id, reason=data.note)
    return {'status': 'open'}


@router.put('/receipt-audit/{dispatch_id}')
def close_receipt(dispatch_id: int, user=Depends(SHIFT)):
    with operational_transaction() as cur:
        cur.execute('''UPDATE receipt_exceptions e JOIN dispatch_records d ON d.id=e.dispatch_id
                       SET e.closed_at=NOW(),e.closed_by=%s WHERE d.id=%s AND d.status='received'
                       AND d.received_at IS NOT NULL AND e.closed_at IS NULL''', (user['sub'], dispatch_id))
        if cur.rowcount != 1:
            raise HTTPException(409, '需先确认收件，再结清待跟进异常')
        audit(cur, user['sub'], 'receipt_reconciled', 'dispatch', dispatch_id)
    return {'status': 'closed'}
