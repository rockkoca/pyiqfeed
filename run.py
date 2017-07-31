#!/usr/bin/python3

from subprocess import *
import re
import time


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
    screens = run_command_with_timeout('screen -ls')
    if screens.startswith('No Sockets found'):
        # create new screen
        print(run_command_with_timeout('screen -dmS index ./base', 10))
        print(run_command_with_timeout('screen -ls'))
        time.sleep(20)
        print(run_command_with_timeout('screen -dmS stock ./lv12', 10))
        run_command_with_timeout('screen -ls')
        # print('screen -S index\ncd pyiqfeed;git pull;python3 base.py')
        # print('screen -S stock\ncd pyiqfeed;git pull;python3 example.py')

        pass
    else:  # locate the screen
        # print(type(screens), screens)
        print('base index')
        print(" screen -r %s" % re.search(r'[0-9]+\.index', screens).group()[:-6])
        print('stock')
        print(" screen -r %s" % re.search(r'[0-9]+\.stock', screens).group()[:-6])
