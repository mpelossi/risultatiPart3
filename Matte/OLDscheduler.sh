#!/bin/bash
echo "Starting asynchronous scheduler..."

# ==========================================
# NODE B QUEUE (4-Core Machine)
# ==========================================
(
    echo "[Node B] Starting blackscholes..."
    kubectl create -f parsec-benchmarks/part3/parsec-blackscholes.yaml
    kubectl wait --for=condition=complete job/parsec-blackscholes --timeout=600s
    
    echo "[Node B] Starting freqmine..."
    kubectl create -f parsec-benchmarks/part3/parsec-freqmine.yaml
    kubectl wait --for=condition=complete job/parsec-freqmine --timeout=600s
    
    echo "[Node B] Queue finished!"
) &   # <--- The '&' runs this entire block in the background


# ==========================================
# NODE A QUEUE (8-Core Machine)
# ==========================================
(
    echo "[Node A] Starting streamcluster..."
    kubectl create -f parsec-benchmarks/part3/parsec-streamcluster.yaml
    kubectl wait --for=condition=complete job/parsec-streamcluster --timeout=600s
    
    echo "[Node A] Starting canneal..."
    kubectl create -f parsec-benchmarks/part3/parsec-canneal.yaml
    kubectl wait --for=condition=complete job/parsec-canneal --timeout=600s
    
    echo "[Node A] Starting barnes..."
    kubectl create -f parsec-benchmarks/part3/parsec-barnes.yaml
    kubectl wait --for=condition=complete job/parsec-barnes --timeout=600s
    
    echo "[Node A] Starting vips..."
    kubectl create -f parsec-benchmarks/part3/parsec-vips.yaml
    kubectl wait --for=condition=complete job/parsec-vips --timeout=600s
    
    echo "[Node A] Starting radix..."
    kubectl create -f parsec-benchmarks/part3/parsec-radix.yaml
    kubectl wait --for=condition=complete job/parsec-radix --timeout=600s
    
    echo "[Node A] Queue finished!"
) &   # <--- The '&' runs this entire block in the background


# ==========================================
# WAIT FOR EVERYTHING TO FINISH
# ==========================================
# The 'wait' command tells the main script to pause here until 
# BOTH background blocks (Node A and Node B) are completely done.
wait 

echo "All batch jobs completed successfully!"