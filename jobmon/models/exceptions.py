class InvalidStateTransition(Exception):
    def __init__(self, model, id, old_state, new_state):
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state)
        super(InvalidStateTransition, self).__init__(self, msg)
