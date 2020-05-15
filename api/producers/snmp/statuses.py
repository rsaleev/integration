import enum
from enum import Enum


class AlmostOutOfPaper(Enum):
    OK = 0
    ALMOST_OUT = 1
    NO_RESPONSE = -1


class BarcodeReader(Enum):
    NO_RESPONSE = 0
    OK = 1
    NOT_USED = -1


class Barrier(Enum):
    OPENED = 1
    CLOSED = 0
    NO_RESPONSE = -1


class Coinbox(Enum):
    CASHBOX_REMOVED = 0
    CASHBOX_PRESENTED = 1
    NO_RESPONSE = -1


class CubeHopper(Enum):
    WAITING_FOR_RESPONSE = -100
    PARTIAL_ACTION = 1
    OK = 0
    ACTION_FAILED_BUSY = -1
    ACTION_FAILED_INTERNAL_ERROR = -2
    ACTION_FAILED_NOT_APPLICABLE = -3
    ACTION_FAILED_COMMUNICATION_ERROR = -4
    ACTION_FAILED_DEVICE_ERROR = -5
    ACTION_FAILED_HALTED = -7


class CCReader(Enum):
    NO_RESPONSE = 0
    OK = 1
    NOT_USED = -1


class CoinsReader(Enum):
    WAITNG_FOR_RESPONSE = -100
    PARTIAL_ACTION = 1
    OK = 0
    ACTION_FAILED_BUSY = -1
    ACTION_FAILED_INTERNAL_ERROR = -2
    ACTION_FAILED_NOT_APPLICABLE = -3
    ACTION_FAILED_COMMUNICATION_ERROR = -4
    ACTION_FAILED_DEVICE_ERROR = -5
    ACTION_FAILED_HALTED = -7
    NO_RESPONSE = -1


class CoinsHopper(Enum):
    WAITNG_FOR_RESPONSE = -100
    PARTIAL_ACTION = 1
    OK = 0
    ACTION_FAILED_BUSY = -1
    ACTION_FAILED_INTERNAL_ERROR = -2
    ACTION_FAILED_NOT_APPLICABLE = -3
    ACTION_FAILED_COMMUNICATION_ERROR = -4
    ACTION_FAILED_DEVICE_ERROR = -5
    ACTION_FAILED_HALTED = -7


class CoinBoxTriggered(Enum):
    COINBOX_UNTRIGGERED = 0
    COINBOX_TRIGGERED = 1
    NO_RESPONSE = -1


class FanIn(Enum):
    OFF = 0
    ON = 1
    NO_RESPONSE = -1


class FanOut(Enum):
    OFF = 0
    ON = 1
    NO_RESPONSE = -1


class General(Enum):
    OUT_OF_SERVICE = 0
    IN_SERVICE = 1
    NO_RESPONSE = -1


class Heater(Enum):
    OFF = 0
    ON = 1
    NO_RESPONSE = -1


class IOBoards(Enum):
    BOARD1_OFFLINE_BOARD2_OFFLINE = 0
    BOARD1_OFFLINE_BOARD2_ONLINE = 1
    BOARD1_ONLINE_BOARD2_OFFLINE = 10
    BOARD1_ONLINE_BOARD2_ONLINE = 11
    BOARD1_ONLINE_BOARD2_ONLINE_BOARD3_ONLINE = 111
    NO_RESPONSE = -1


class Loop(Enum):
    FREE = 0
    OCCUPIED = 1
    NO_RESPONSE = -1


class MiddleDoor(Enum):
    OPEN = 1
    CLOSED = 0
    NO_RESPONSE = -1


class NotesEscrow(Enum):
    NO_ERROR = 0
    COINS_LOW = 3
    JAMMED_NOTE = 5
    TIMEOUT = 9
    INCOMPLETE_PAYOUT = 10
    INCOMPLETE_FLOAT = 11
    FRAUD_ATTEMPT = 12
    NOTE_PATH_JAMMED = 26
    NOTE_STUCK_JAMMED = 27
    CASHBOX_FULL = 30
    CASHBOX_REMOVED = 31
    LID_OPENED = 33
    NO_USED = 35
    CALIBRATION_FAULT = 36
    ATTACHED_MECH_JAM = 37
    ATTACHED_MECH_OPEN = 38
    ACTION_FAILED_BUSY = -1
    ACTION_FAILED_INTERNAL_ERROR = -2
    ACTION_FAILED_NOT_APPLICABLE = -3
    ACTION_FAILED_NOT_SUPPORTED = -4
    ACTION_FAILED_COMMUNICATION_ERROR = -5
    ACTION_FAILED_DEVICE_ERROR = -6
    ACTION_FAILED_HALTED = -7
    STILL_WAITING_FOR_1ST_RESPONSE = -100


class NotesReader(Enum):
    NO_ERROR = 0
    JAMMED_NOTE = 1
    COINS_LOW = 3
    JAMMED_NOTE_TRAP = 5
    TIMEOUT = 9
    INCOMPLETE_PAYOUT = 10
    INCOMPLETE_FLOAT = 11
    FRAUD_ATTEMPT = 12
    NOTE_PATH_JAMMED = 26
    NOTE_STUCK_JAMMED = 27
    CASHBOX_FULL = 30
    CASHBOX_REMOVED = 31
    LID_OPENED = 33
    NO_USED = 35
    CALIBRATION_FAULT = 36
    ATTACHED_MECH_JAM = 37
    ATTACHED_MECH_OPEN = 38
    STILL_WAITING_FOR_1ST_RESPONSE = -100
    NO_RESPONSE = -1


class PaperDevice(Enum):
    OUT_OF_PAPER = 0
    PAPER_PRESENTED = 1
    ALMOST_OUT_OF_PAPER = 2
    NO_RESPONSE = -1


class Roboticket(Enum):
    OFFLINE = -3
    POSITION_ERROR = -2
    NOT_INITIALIZED = -1
    NO_ERROR = 0
    COMMAND_IN_EXECUTION = 1
    COMMAND_NOT_EXECUTED_FOR_TITLE_NOT_PRESENT = 2
    COMMAND_NOT_EXECUTED_FOR_TITLE_ALREADY_PRESENTED_IN_MODULE = 3
    AUTOMATIC_READ_ALWAYS_ON = 4
    CUTTER_ERROR = 5
    TITLE_JAMMED = 7
    CONFIGURATION_NOT_VALID = 8
    OPTIONAL_DEVICE_NOT_INSTALLED = 9


class TicketPrinter(Enum):
    UNKNOWN = - 100
    OFFLINE = -1
    OK = 0
    PAPER_LEFT_IN_ROLLER = 1
    CUTTER_JAMMED = 2
    OUT_OF_PAPER = 3
    PRINTHEAD_LIFTED = 4
    PAPER_FEED_ERROR = 5
    TEMPERATURE_ERROR = 6
    ROLLER_NOT_RUNNING = 7
    PAPER_JAMMED_DURING_FEED = 8
    BLACK_MARK_NOT_FOUND = 10
    BLACK_MARK_CALIBRATION_ERROR = 11
    INDEX_ERROR = 12
    CHECKSUM_ERROR = 13
    WRONG_FIRMWARE = 14
    CANT_START_FIRMWARE = 15
    FEED_TIMED_OUT = 16


class TicketReader(Enum):
    OFFLINE = -2
    NOT_INIT = -1
    NO_ERROR = 0
    COMMAND_IN_EXECUTION = 1
    COMMAND_NOT_EXECUTED_FOR_PRINTHEAD_NOT_IN_MODULE = 2
    COMMAND_NOT_EXECUTED_FOR_PRINTHEAD_IN_MODULE = 3
    COMMAND_NOT_EXECUTED_FOR_READING_ENABLED = 4
    PRINTHEAD_JAMMED = 7


class UppperDoor(Enum):
    OPEN = 1
    CLOSED = 0
    NO_RESPONSE = -1


class VoIP(Enum):
    UP = 1
    DOWN = 0
    NO_RESPONSE = -1


class Ups(Enum):
    OFF = 0
    ON = 1
    NO_RESPONSE = -1


class IOCCTalk(Enum):
    WAIITNG_FOR_RESPONSE = -100
    PARTIAL_ACTION = 1
    OK = 0
    ACTION_FAILED_BUSY = -1
    ACTION_FAILED_INTERNAL_ERROR = -2
    ACTION_FAILED_NOT_APPLICABLE = -3
    ACTION_FAILED_COMMUNICATION_ERROR = -4
    ACTION_FAILED_DEVICE_ERROR = -5
    ACTION_FAILED_HALTED = -7


class FiscalPrinter(Enum):
    NO_RESPONSE = 0
    OK = 1


class FiscalPrintingError(Enum):
    PAPER_PRESENTED = 0
    ERROR = 1
    OUT_OF_PAPER = 2
    NO_RESPONSE = -1


class FiscalPrinterBD(Enum):
    UNKNOWN = -1
    SHIFT_OPEN_24H_NOT_ELAPSED = 2
    SHIFT_OPEN_24H_ELAPSED = 3
    SHIFT_CLOSED = 4


class PaymentType(Enum):
    CASH = 1
    CARD = 2
    TROIKA = 3


class PaymentCardType(Enum):
    VISA = 1
    MAESTRO = 2
    MASTERCARD = 3
    MIR = 4


class PaymentStatus(Enum):
    FINISHED_WITH_SUCCESS = 0
    ZONE_PAYMENT = 1
    PAYMENT_CANCELLED = 2
    FINISHED_WITH_ISSUES = 3
    OPERATION_TIMEOUT = 4
    UNKNOWN = 5
