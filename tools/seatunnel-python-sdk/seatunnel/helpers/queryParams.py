from dataclasses import dataclass

class SubmitJobQueryParams:
    def __init__(self, jobId: str | None = None, jobName: str | None = None,
                isStartWithSavePoint: str | None = None, format: str | None = "hocon"):
        self.jobId = jobId
        self.jobName = jobName
        self.isStartWithSavePoint = isStartWithSavePoint
        self.format = format

class SubmitJobFileQueryParams:
    def __init__(self, jobId: int | None = None, jobName: str | None = None,
                isStartWithSavePoint: str | None = None):
        self.jobId = jobId
        self.jobName = jobName
        self.isStartWithSavePoint = isStartWithSavePoint

class StopJobQueryParams:
    def __init__(self, jobId: int, isStartWithSavePoint: bool):
        self.jobId = jobId
        self.isStartWithSavePoint = isStartWithSavePoint

@dataclass
class OverviewQueryParams:
    tagName: str
    tagValue: str