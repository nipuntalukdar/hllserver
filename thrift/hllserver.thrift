namespace go hllthrift

enum Status {
    SUCCESS,
    FAILURE,
    KEY_EXISTS,
    KEY_NOT_EXISTS
}

struct AddLogCmd {
    1: string Key,
    2: i64 Expiry = 0
}

struct UpdateLogCmd {
    1: string Key,
    2: binary Data,
}

struct UpdateLogMValCmd {
    1: string Key,
    2: list<binary> Data,
}

struct UpdateExpiryCmd {
    1: string Key,
    2: i64 Expiry
}


struct CardinalityResponse {
    1: string Key
    2: Status Status
    3: i64 Cardinality
}

service HllService {
    Status AddLog(1:AddLogCmd addLog)
    Status Update(1:UpdateLogCmd upd)
    Status UpdateM(1:UpdateLogMValCmd mupd)
    Status UpdateExpiry(1:UpdateExpiryCmd exp)
    Status DelLog(1:string key)
    CardinalityResponse GetCardinality(1:string Key)
}
