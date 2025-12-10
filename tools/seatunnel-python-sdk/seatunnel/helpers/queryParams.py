from dataclasses import dataclass

@dataclass
class SubmitJobQueryParams:
    jobId: int | None = None
    jobName: str | None = None
    isStartWithSavePoint: bool | None = None
    format: str = "hocon"

@dataclass
class SubmitJobFileQueryParams:
    jobId: int | None = None
    jobName: str | None = None
    isStartWithSavePoint: bool | None = None

@dataclass
class StopJobQueryParams:
    jobId: int
    isStartWithSavePoint: bool

@dataclass
class OverviewQueryParams:
    tagName: str
    tagValue: str