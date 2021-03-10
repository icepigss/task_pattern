         
                  / worker1 \   
                 /           \
start -- produce -- worker2 -- merge -- end
                 \           /
                  \ worker3 /

- can control speed by producer.
- concurrent workers make sure it won't block.
- smooth startup an end.
