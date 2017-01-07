class TransportError(Exception):
    pass


class TransportLoadError(TransportError):
    pass


class TransportRequirementError(TransportError):
    pass
