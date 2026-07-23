PRAGMA foreign_keys = ON;

-- Collection item state is high-volume but is still part of the authoritative
-- lifecycle. SQLite triggers keep the mutation, audit row, and durable OTLP-log
-- outbox row in one transaction, including bulk page ingestion.
CREATE TRIGGER batch_item_observability_insert
AFTER INSERT ON batch_items
BEGIN
    INSERT INTO audit_events(
        entity_type,entity_id,event_type,metadata_json,occurred_at
    )
    VALUES (
        'batch_item',
        NEW.id,
        'batch_item.created',
        json_object(
            'event_id', lower(hex(randomblob(16))),
            'batch_id', NEW.batch_id,
            'run_id', NEW.run_id,
            'state', NEW.state
        ),
        NEW.created_at
    );

    INSERT INTO observability_outbox(
        event_id,audit_event_id,entity_type,entity_id,event_name,
        attributes_json,occurred_at,traceparent,tracestate,next_attempt_at
    )
    SELECT
        json_extract(a.metadata_json,'$.event_id'),
        a.id,
        'batch_item',
        NEW.id,
        'batch_item.created',
        json_object(
            'event.id', json_extract(a.metadata_json,'$.event_id'),
            'event.sequence', a.id,
            'event.name', 'batch_item.created',
            'event.occurred_at', NEW.created_at,
            'entity.type', 'batch_item',
            'entity.id', NEW.id,
            'schedule.id', b.schedule_id,
            'trigger.id', b.trigger_identity_id,
            'batch.id', NEW.batch_id,
            'item.id', NEW.id,
            'run.id', NEW.run_id,
            'operations.timezone', t.operations_timezone,
            'operations.day', t.operations_day,
            'completion.deadline_at', t.completion_deadline_at,
            'state.from', NULL,
            'state.to', NEW.state
        ),
        NEW.created_at,
        t.traceparent,
        t.tracestate,
        NEW.created_at
    FROM audit_events a
    JOIN batches b ON b.id=NEW.batch_id
    JOIN trigger_identities t ON t.id=b.trigger_identity_id
    WHERE a.id=last_insert_rowid();
END;

CREATE TRIGGER batch_item_observability_state
AFTER UPDATE OF state ON batch_items
WHEN OLD.state<>NEW.state
BEGIN
    INSERT INTO audit_events(
        entity_type,entity_id,event_type,metadata_json,occurred_at
    )
    VALUES (
        'batch_item',
        NEW.id,
        'batch_item.state_changed',
        json_object(
            'event_id', lower(hex(randomblob(16))),
            'batch_id', NEW.batch_id,
            'run_id', NEW.run_id,
            'from', OLD.state,
            'to', NEW.state
        ),
        NEW.updated_at
    );

    INSERT INTO observability_outbox(
        event_id,audit_event_id,entity_type,entity_id,event_name,
        attributes_json,occurred_at,traceparent,tracestate,next_attempt_at
    )
    SELECT
        json_extract(a.metadata_json,'$.event_id'),
        a.id,
        'batch_item',
        NEW.id,
        'batch_item.state_changed',
        json_object(
            'event.id', json_extract(a.metadata_json,'$.event_id'),
            'event.sequence', a.id,
            'event.name', 'batch_item.state_changed',
            'event.occurred_at', NEW.updated_at,
            'entity.type', 'batch_item',
            'entity.id', NEW.id,
            'schedule.id', b.schedule_id,
            'trigger.id', b.trigger_identity_id,
            'batch.id', NEW.batch_id,
            'item.id', NEW.id,
            'run.id', NEW.run_id,
            'operations.timezone', t.operations_timezone,
            'operations.day', t.operations_day,
            'completion.deadline_at', t.completion_deadline_at,
            'state.from', OLD.state,
            'state.to', NEW.state
        ),
        NEW.updated_at,
        t.traceparent,
        t.tracestate,
        NEW.updated_at
    FROM audit_events a
    JOIN batches b ON b.id=NEW.batch_id
    JOIN trigger_identities t ON t.id=b.trigger_identity_id
    WHERE a.id=last_insert_rowid();
END;
