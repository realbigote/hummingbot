#!/bin/bash
tmux new-session -d -s hummingbot -n hbstrat
tmux send-keys -t hummingbot:hbstrat "python3 bin/hummingbot_quickstart.py" Enter
#GET PID by parsing?? or Force PID
#TAIL PID
tail -f /dev/null# # To attach run 
#tmux attach -t hummingbot:hbstrat


