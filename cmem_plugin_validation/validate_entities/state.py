"""JSON validation process state"""


class State:
    """State of a validation process"""

    def __init__(self):
        self.total = 0
        self.violations = 0
        self.violations_messages = []

    def increment_total(self) -> None:
        """Increment total"""
        self.total += 1

    def add_violations_message(self, message: str) -> None:
        """Add violation message"""
        self.violations += 1
        self.violations_messages.append(message)
