from easypy.exceptions import TException
from easypy.timing import PredicateNotSatisfied


class CommandExecutionError(TException):
    template = "Command execution error: {name}"


class UnknownChannel(CommandExecutionError):
    template = "Unknown channel {channel}"


class UnknownExitCode(CommandExecutionError):
    template = "Unknown exit code {exit_code}"


class CommandTimeoutError(CommandExecutionError):
    template = "Command timed out after {timeout}: {name}"


class CommandPidTimeoutError(TimeoutError):
    pass


class CommandAlreadyDone(TException):
    template = "Command is already done"


class CommandAbortedByReboot(CommandExecutionError):
    template = "Command aborted by reboot request: {name}"


class CommandAbortedByOverflow(CommandExecutionError):
    template = "Command aborted because it had too much output: {name}"


class CommandOrphaned(CommandExecutionError):
    template = "Talker agent restarted while command was running: {name}"


class TalkerError(CommandExecutionError):
    template = "Talker agent encountered an error: `{_exception}`"


class ClientCommandTimeoutError(CommandExecutionError):
    template = "Command timed out after {timeout!r}: {name} - probably hanging on the host"


class TalkerClientSendTimeout(CommandExecutionError):
    template = "Talker client did not send the command within {timeout!r}: this machine is probably overloaded"


class TalkerServerTimeout(CommandExecutionError):
    template = "Talker agent did not receive command after {timeout!r}: either host or talker is down"


class TalkerCommandLost(CommandExecutionError):
    template = "Talker agent did not receive command after {timeout!r}, though host and talker are alive"


class CommandTimeout(CommandExecutionError):
    template = "Command did not finish within {timeout!r}: {name} - probably hanging on the host"


class CommandLineTimeout(CommandExecutionError):
    template = "Command did not send output for {line_timeout!r}: {name}"


class HostStillAlive(TException):
    template = "Host {hostname} did not went down within {elapsed}"


class HostDidNotRecover(CommandExecutionError):
    template = "Host {hostname} did not recover from reboot within {timeout}"


class TalkerProcessLineTimedOut(TException):
    template = "{_msg} ({machine})"


class RedisTimeoutError(TException):
    template = "Talker's redis server connection timed out {talker}"


class NoResponseForRedisCommand(TException, PredicateNotSatisfied):
    template = "No response for redis command within timeout: {cmd}"


class RedisConnectionError(TException):
    template = "Connection error while communicating with Talker Redis {talker}"
