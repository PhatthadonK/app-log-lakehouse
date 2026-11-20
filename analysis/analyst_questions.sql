-- =====================================================================
-- App Log Lakehouse: analyst questions
-- Sprint 1: questions + expected outputs only.
-- Sprint 3: write the SQL under each question against the gold tables.
-- =====================================================================

-- Q1. Daily and weekly active users (DAU / WAU)
--   Output : date, dau, wau
--   Source : fact_events JOIN dim_date
--   Notes  : count DISTINCT user_key; exclude events with unknown user


-- Q2. Conversion funnel by device: view_item -> add_to_cart -> purchase
--   Output : device, sessions_viewed, sessions_added_to_cart, sessions_purchased,
--            view_to_cart_pct, cart_to_purchase_pct, view_to_purchase_pct
--   Source : fact_events (or fct_sessions)
--   Notes  : funnel is per session, not per event


-- Q3. Top 10 items by revenue per week
--   Output : week, rank, item_id, category, revenue, units
--   Source : fact_events JOIN dim_item JOIN dim_date
--   Notes  : only purchase events; use a window function for rank


-- Q4. Average session length and events per session
--   Output : date, sessions, avg_session_minutes, avg_events_per_session
--   Source : fct_sessions (or fact_events grouped by session_id)


-- Q5. D7 retention: % of new users who come back within 7 days
--   Output : cohort_date, new_users, retained_d7, d7_retention_pct
--   Source : dim_user (first_seen_date) JOIN fact_events
--   Notes  : "come back" = any event on day 1..7 after first_seen_date