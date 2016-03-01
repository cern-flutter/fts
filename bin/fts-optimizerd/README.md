fts-optimizerd
==============
This daemon decides how many transfers a link can sustain. It subscribes to
the termination messages coming from the workers, and decide if the number of
actives should be increased, decreased or kept depending on the evolution.

When it takes a decision, it stores the decision so the scheduler knows the
working range for each link.
