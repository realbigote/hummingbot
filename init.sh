#!/bin/bash
tmux new-session -d -s hummingbot -n hbstrat
tmux send-keys -t hummingbot:hbstrat " /opt/conda/envs/hummingbot/bin/python3 /bin/hummingbot_quickstart.py" Enter
tail -f /dev/null

# # To attach run 
#tmux attach -t hummingbot:hbstrat


