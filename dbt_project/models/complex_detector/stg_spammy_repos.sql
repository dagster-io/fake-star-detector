-- ### suspicious activity cluster definition ###
-- suspicious repos
{% set repo_actor_overlap_gte = 4 %} -- at least x actors from this set interacted with repo OR
{% set min_repo_actor_overlap_gte = 3 %} -- y actors from this set interacted with repo AND
{% set min_p_actor_overlap_gte = 0.5 %}  -- at least z% of this repos' interactions were from those y actors

-- identify suspicious repos (activity cluster heuristic)
SELECT repo
FROM {{ ref('stg_starring_actor_repo_clusters') }}
WHERE -- at least x overlapping actors from this set interacted with repo
    n_actor_overlap >= {{repo_actor_overlap_gte}}
    OR ( -- fewer actors, but at least z% of this repos' interactions were from those actors
        n_actor_overlap >= {{min_repo_actor_overlap_gte}}
        AND p_actor_overlap >= {{min_p_actor_overlap_gte}}
    )
