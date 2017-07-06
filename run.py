#!/usr/bin/python3

from subprocess import *


def run_command_with_timeout(cmd, timeout_sec=5, project_path=None):
    """Execute `cmd` in a subprocess and enforce timeout `timeout_sec` seconds.

    Return subprocess exit code on natural completion of the subprocess.
    Raise an exception if timeout expires before subprocess completes."""
    # print(cmd)
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    try:
        outs, errs = proc.communicate(timeout=timeout_sec)
        # print(outs, errs)
        # print(str(outs), errs)
        outs += errs
        try:
            outs = outs.decode('utf-8')
        except Exception as e:
            # print(e)
            pass
        return str(outs).replace('\r\n', '\n')  # [:20]

    except TimeoutExpired:
        proc.kill()
        # outs, errs = proc.communicate()

        raise Exception('   => Process #%d killed after %f seconds' % (proc.pid, timeout_sec))


if __name__ == '__main__':
    print(run_command_with_timeout('screen -ls'))
