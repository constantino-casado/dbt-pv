#!/usr/bin/env python

import os
import uuid
from pvcli.pvcli import PurviewClient
import logging,sys
import platform
import time
import json
import subprocess
from datetime import datetime, timezone
from typing import Optional, List
from pvcli.run import RunEvent, RunState, Run, Job
from provider.dbt import DbtArtifactProcessor, ParentRunMetadata
from tqdm import tqdm

__version__ = "0.1"

ADB = "ADB-fqdn"
PRODUCER = f"https://{ADB}/dbt/v{__version__}"

def parse_single_arg(args, keys: List[str], default=None) -> Optional[str]:
    """
    In provided argument list, find first key that has value and return that value.
    Values can be passed either as one argument {key}={value}, or two: {key} {value}
    """
    for key in keys:
        for i, arg in enumerate(args):
            if arg == key and len(args) > i:
                return args[i+1]
            if arg.startswith(f"{key}="):
                return arg.split("=", 1)[1]
    return default

print("-- setting variables --")
# When run in my windows I'll load variables, otherwise will take them from container env variables in cloud
if platform.uname().system == 'Windows':
    setvars=os.path.expanduser('~\\.secrets\\setvariables.py')
    exec(open(setvars).read())
PurviewName = os.environ.get("PURVIEW_NAME") 


def dbt_run_event(state: RunState, job_name: str, job_namespace: str, run_id: str = str(uuid.uuid4()), parent: Optional[ParentRunMetadata] = None)->RunEvent:
    return RunEvent(eventType=state,eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "parent": parent.to_openlineage()
            } if parent else {}
        ),
        job=Job(
            namespace=parent.job_namespace if parent else job_namespace,
            name=job_name
        ),
        producer=PRODUCER
    )


#
def dbt_run_event_start(job_name: str, job_namespace: str, parent_run_metadata: ParentRunMetadata)-> RunEvent:
    return dbt_run_event(
        state=RunState.START,
        job_name=job_name,
        job_namespace=job_namespace,
        parent=parent_run_metadata
    )


# 
def dbt_run_event_end(
    run_id: str,
    job_namespace: str,
    job_name: str,
    parent_run_metadata: Optional[ParentRunMetadata]
)-> RunEvent:
    return dbt_run_event(
        state=RunState.COMPLETE,
        job_namespace=job_namespace,
        job_name=job_name,
        run_id=run_id,
        parent=parent_run_metadata
    )

logger = logging.getLogger("dbtol")
logger.setLevel("INFO")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info(f"Starting process pointing to <{PurviewName}> catalog account")

def main():
    logger.info(f"Running Purview dbt wrapper version {__version__}")
    logger.info(f"This wrapper will send Purview events at the end of dbt execution.")

    args = sys.argv[2:]
    target = parse_single_arg(args, ['-t', '--target'])
    project_dir = parse_single_arg(args, ['--project-dir'], default='./')
    profile_name = parse_single_arg(args, ['--profile'])
    print(f"project directory in main: {project_dir}")
    # The print command just for "testing"
    # print(f"target {target}"); print(f"project_dir {project_dir}"); print(f"profile_name {profile_name}")

    # We can get this if we have been orchestrated by an external system like airflow
    parent_id = os.getenv("EXECUTION_PARENT_ID")
    parent_run_metadata = None
    job_namespace = os.environ.get(
        'EXECUTION_NAMESPACE',
        'dbt'
    )

    # Client object class from Purview, to be used and created inside its module 
    client = PurviewClient.from_environment()

    if parent_id:
        parent_namespace, parent_job_name, parent_run_id = parent_id.split('/')
        parent_run_metadata = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace
        )

    processor = DbtArtifactProcessor(
        producer=PRODUCER,
        target=target,
        job_namespace=job_namespace,
        project_dir=project_dir, # type: ignore        profile_name=profile_name,
        logger=logger
    )

    # Always emit "wrapping event" around dbt run. This indicates start of dbt execution, since
    # both the start and complete events for dbt models won't get emitted until end of execution.
    print(f"-- -- dbt-run-{processor.project['name']}")
    start_event = dbt_run_event_start(
        job_name=f"dbt-run-{processor.project['name']}",
        job_namespace=job_namespace,
        parent_run_metadata=parent_run_metadata
    )
    dbt_run_metadata = ParentRunMetadata(
        run_id=start_event.run.runId,
        job_name=start_event.job.name,
        job_namespace=start_event.job.namespace
    )

    # Failed start event emit should not stop dbt command from running.
    try:
        client.emit(event=start_event)
    except Exception as e:
        logger.warning("OpenLineage client failed to emit start event. Exception: %s", e)

    # Set parent run metadata to use it as parent run facet
    processor.dbt_run_metadata = dbt_run_metadata

    pre_run_time = time.time()
    # Execute dbt in external process
    process = subprocess.Popen(
        ["dbt"] + sys.argv[1:],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    process.wait()

    if len(sys.argv) < 2 or sys.argv[1] not in ['run', 'test', 'build']:
        return

    # If run_result has modification time before dbt command
    # or does not exist, do not emit dbt events.
    try:
        if os.stat(processor.run_result_path).st_mtime < pre_run_time:
            logger.info(f"OpenLineage events not emitted: run_result file "
                        f"({processor.run_result_path}) was not modified by dbt")
            return
    except FileNotFoundError:
        logger.info(f"OpenLineage events not emitted:"
                    f"did not find run_result file ({processor.run_result_path})")
        return

    events = processor.parse().events()

    end_event = dbt_run_event_end(
        run_id=dbt_run_metadata.run_id,
        job_namespace=dbt_run_metadata.job_namespace,
        job_name=dbt_run_metadata.job_name,
        parent_run_metadata=parent_run_metadata
    )

    for event in tqdm(
        events + [end_event],
        desc="Emitting Lineage events",
        initial=1,
        total=len(events) + 2,
    ):
        client.emit(event)
    client.send2pv(processor)
    # client.debug()
    # print(json.dumps(processor.sources))
    logger.info(f"Emitted {len(events) + 2} lineage events")


if __name__ == '__main__':
    main()
