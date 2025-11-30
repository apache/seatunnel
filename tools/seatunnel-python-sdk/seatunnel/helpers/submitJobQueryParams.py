class SubmitJobQueryParams:
    def __init__(self, jobId: str | None = None, jobName: str | None = None,
                isStartWithSavePoint: str | None = None, format: str | None = "hocon"):
        self.jobId = jobId
        self.jobName = jobName
        self.isStartWithSavePoint = isStartWithSavePoint
        self.format = format

class SubmitJobFileQueryParams:
    def __init__(self, jobId: str | None = None, jobName: str | None = None,
                isStartWithSavePoint: str | None = None):
        self.jobId = jobId
        self.jobName = jobName
        self.isStartWithSavePoint = isStartWithSavePoint
