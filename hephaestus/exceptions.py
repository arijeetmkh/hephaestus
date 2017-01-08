class TransportError(Exception):
    pass


class TransportNotFound(TransportError):
    pass


class TransportLoadError(TransportError):
    pass


class TransportRequirementError(TransportError):
    pass
