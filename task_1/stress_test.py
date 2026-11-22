"""
Task 1 - Reproducible stress script
===================================

Fires 30 concurrent client threads (>= 25 required) at the router while
4 worker threads drain the list queue, then checks the results against
what MUST be true if the code were correct.

Usage:
    python stress_test.py                 # broken router, 3 trials
    python stress_test.py --fixed         # fixed router (all checks pass)
    python stress_test.py --trials 5 --clients 40

Output: printed report + results/task1_<broken|fixed>_report.txt
"""
