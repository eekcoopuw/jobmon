"""DB side exceptions."""


class InvalidStateTransition(Exception):
    """Invalid State Transition implementation."""

    def __init__(self, model: str, id: str, old_state: str, new_state: str) -> None:
        """Initialize Exception."""
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state
        )
        super(InvalidStateTransition, self).__init__(self, msg)


class KillSelfTransition(Exception):
    """Exception to signal kill self is necessary."""

    def __init__(self, model: str, id: str, old_state: str, new_state: str) -> None:
        """Initialize Exception."""
        msg = (
            f"Cannot transition {model} id: {id} from {old_state} to "
            f"{new_state} because it was in a lost track or unknown state "
            f"and must kill itself"
        )
        super(KillSelfTransition, self).__init__(self, msg)
