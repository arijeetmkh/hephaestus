class TransportError(Exception):
    pass


class TransportFileNotFound(FileNotFoundError):
    pass


class TransportNotFound(TransportError):
    pass


class TransportLoadError(TransportError):
    pass


class TransportRequirementError(TransportError):
    pass


class ReceiverError(Exception):
    pass
