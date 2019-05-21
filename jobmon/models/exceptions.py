class InvalidStateTransition(Exception):
    def __init__(self, model, id, old_state, new_state):
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state)
        super(InvalidStateTransition, self).__init__(self, msg)


class KillSelfTransition(Exception):
    def __init__(self, model, id, old_state, new_state):
        msg = f"Cannot transition {model} id: {id} from {old_state} to " \
              f"{new_state} because it was in a lost track or unknown state " \
              f"and must kill itself"
        super(KillSelfTransition, self).__init__(self, msg)
