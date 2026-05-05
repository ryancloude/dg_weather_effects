from __future__ import annotations

try:
    from pipeline_monitor.report_pipeline_run import handle_execution_status_event
except ImportError:
    from report_pipeline_run import handle_execution_status_event


def handler(event, context):
    del context
    return handle_execution_status_event(event)
