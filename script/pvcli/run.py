
import uuid
from enum import Enum
from typing import Dict, List, Optional

import attr
from dateutil import parser

class RedactMixin:
    _skip_redact: List[str] = []

    @property
    def skip_redact(self) -> List[str]:
        return self._skip_redact


@attr.s
class BaseFacet(RedactMixin):
    _producer: str = attr.ib(init=False)
    _schemaURL: str = attr.ib(init=False)

    _base_skip_redact: List[str] = ['_producer', '_schemaURL']
    _additional_skip_redact: List[str] = []

    def __attrs_post_init__(self):
        self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/BaseFacet"

    @property
    def skip_redact(self):
        return self._base_skip_redact + self._additional_skip_redact


@attr.s
class NominalTimeRunFacet(BaseFacet):
    nominalStartTime: str = attr.ib()
    nominalEndTime: Optional[str] = attr.ib(default=None)

    _additional_skip_redact: List[str] = ['nominalStartTime', 'nominalEndTime']

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/NominalTimeRunFacet"


@attr.s
class ParentRunFacet(BaseFacet):
    run: Dict = attr.ib()
    job: Dict = attr.ib()

    _additional_skip_redact: List[str] = ['job', 'run']

    @classmethod
    def create(cls, runId: str, namespace: str, name: str):
        return cls(
            run={
                "runId": runId
            },
            job={
                "namespace": namespace,
                "name": name
            }
        )

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ParentRunFacet"



class RunState(Enum):
    START = 'START'
    RUNNING = 'RUNNING'
    COMPLETE = 'COMPLETE'
    ABORT = 'ABORT'
    FAIL = 'FAIL'
    OTHER = 'OTHER'


_RUN_FACETS = [
    NominalTimeRunFacet,
    ParentRunFacet
]


@attr.s
class Run(RedactMixin):
    runId: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['runId']

    @runId.validator
    def check(self, attribute, value):
        uuid.UUID(value)


@attr.s
class Job(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['namespace', 'name']


@attr.s
class Dataset(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['namespace', 'name']


@attr.s
class InputDataset(Dataset):
    inputFacets: Dict = attr.ib(factory=dict)


@attr.s
class OutputDataset(Dataset):
    outputFacets: Dict = attr.ib(factory=dict)


@attr.s
class RunEvent(RedactMixin):
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))
    eventTime: str = attr.ib()
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)     # type: ignore
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)    # type: ignore

    _skip_redact: List[str] = ['eventType', 'eventTime', 'producer']

    @eventTime.validator
    def check(self, attribute, value):
        parser.isoparse(value)
        if 't' not in value.lower():
            # make sure date-time contains time
            raise ValueError("Parsed date-time has to contain time: {}".format(value))
