import uuid

class ServerLog():
    def __init__(self, eventId: str, accountId: int, eventType: str, locationCountry: str, eventTimeStamp: int) -> None:
        self.eventId = eventId
        self.accountId = accountId
        self.eventType = eventType
        self.locationCountry = locationCountry
        self.eventTimeSteamp = eventTimeStamp

    def toString(self):
        return f"{self.eventId}, {self.accountId}, {self.eventType}, {self.locationCountry}, {self.eventTimeSteamp}"

def fromString(value:str) -> ServerLog:
    elements = value.split(",")
    return ServerLog(elements[0], int(elements[1]), elements[2], elements[3], int(elements[4]))


