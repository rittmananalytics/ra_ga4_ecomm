# GA4 E-commerce Analytics - dbt Package

A comprehensive dbt package for transforming Google Analytics 4 e-commerce data into analytics-ready models with web analytics, e-commerce, and conversion rate analytics metrics.

## 📊 Models

### Mart Models

All models are **incremental** with partitioning and clustering for optimal performance in BigQuery.

#### 1. **sessions**
Session-level aggregations from GA4 events.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Key Metrics**:
  - Page views, events, and engagement time per session
  - Session duration
  - Purchase events and revenue per session
  - Conversion flags
  - Traffic source attribution
  - Device and geographic context

#### 2. **pageviews**
Individual page view events with web analytics metrics.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Clustering**: By `user_pseudo_id`, `ga_session_id`
- **Key Metrics**:
  - Page path, hostname, query string
  - Time on page
  - Navigation flow (previous/next page)
  - Entry/exit page flags
  - Page position in session
  - Engagement metrics

#### 3. **users**
User-level aggregations with RFM segmentation and lifetime value.
- **Materialization**: Table (updated via dependencies)
- **Key Metrics**:
  - Total sessions, page views, events
  - Lifetime revenue and conversion metrics
  - Average order value, revenue per session
  - RFM segmentation (Recency, Frequency, Monetary)
  - User type classification (Repeat Buyer, One-Time Buyer, etc.)
  - First-touch and last-touch attribution
  - Engagement metrics

#### 4. **add_to_cart_events**
Product add-to-cart events with item details.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Clustering**: By `user_pseudo_id`, `ga_session_id`
- **Key Metrics**:
  - Product details (ID, name, brand, variant, category)
  - Pricing and quantity
  - Cart value
  - Promotional information
  - Traffic source and device context

#### 5. **purchase_events**
Purchase transactions with revenue analytics.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Clustering**: By `user_pseudo_id`, `transaction_id`
- **Key Metrics**:
  - Transaction details and revenue
  - Product-level purchase data
  - Time to purchase from first visit
  - Purchase timing (hour of day, day of week)
  - Tax and shipping values
  - Traffic source attribution

#### 6. **product_views**
Product detail page views.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Clustering**: By `user_pseudo_id`, `ga_session_id`
- **Key Metrics**:
  - Product information
  - Pricing
  - List and promotion context
  - Category hierarchy

#### 7. **conversion_funnel**
Daily conversion funnel metrics with drop-off analysis.
- **Materialization**: Incremental
- **Partitioning**: By `event_date` (daily)
- **Key Metrics**:
  - Session and user counts at each funnel stage:
    - Page view → Product view → Add to cart → Begin checkout → Purchase
  - Step-by-step conversion rates
  - Overall conversion rate
  - Drop-off counts and rates at each stage
  - Segmented by traffic source, device, and geography

## 📁 Project Structure

```
ra_ga4_ecomm/
├── models/
│   ├── marts/
│   │   ├── sessions.sql
│   │   ├── pageviews.sql
│   │   ├── users.sql
│   │   ├── add_to_cart_events.sql
│   │   ├── purchase_events.sql
│   │   ├── product_views.sql
│   │   ├── conversion_funnel.sql
│   │   └── schema.yml
│   └── sources.yml
└── analyses/
    └── data_quality_validation.sql
```

## 🚀 Quick Start

### 1. Run All Models (Full Refresh)
```bash
dbt run --full-refresh
```

### 2. Run All Models (Incremental)
```bash
dbt run
```

### 3. Run Tests
```bash
dbt test
```

### 4. Run Data Quality Validation
```bash
dbt compile --select data_quality_validation
bq query --use_legacy_sql=false < target/compiled/ra_ga4_ecomm/analyses/data_quality_validation.sql
```

## 📈 Data Quality

All models include:
- **Primary key tests** on critical fields
- **Not null tests** on required columns
- **Cross-model consistency checks**
- **Logical validation** (no negative values where inappropriate)

### Validation Results (Latest)
- ✅ **1.35M** page views processed
- ✅ **361K** sessions aggregated
- ✅ **270K** unique users identified
- ✅ **667K** add-to-cart events tracked
- ✅ **2.75M** product views recorded
- ✅ **16K** purchase items processed
- ✅ **$362K** total revenue tracked
- ✅ **4,419** users with purchases
- ✅ **775** repeat buyers
- ✅ **19/19** data quality tests passing

### Known Data Characteristics
- Some purchase events may have null `transaction_id` (59 out of 16K) - this is present in the source data
- Funnel progressions may not be strictly hierarchical (users can skip steps in GA4 e-commerce tracking)
- Small rounding differences (~$55 out of $362K) between revenue aggregations are expected

## 🏗️ Incremental Loading

All event-based models support incremental loading:
- Only processes new data based on `event_date`
- Uses partition pruning for cost efficiency
- Maintains unique keys to prevent duplicates
- Automatically handles late-arriving data

To process only new data:
```bash
dbt run
```

To rebuild everything:
```bash
dbt run --full-refresh
```

## 📊 Key Business Metrics

### Web Analytics
- **Sessions**: 361K
- **Page Views**: 1.35M
- **Avg Pages per Session**: 3.7
- **Bounce Rate**: Calculated from entrance/exit pages

### E-commerce
- **Conversion Rate**: 1.34% (4,851 conversions / 361K sessions)
- **Average Order Value**: $59.76
- **Revenue per Session**: $1.00
- **Product Views**: 2.75M
- **Add to Cart**: 667K
- **Purchases**: 16K items

### User Behavior
- **Total Users**: 270K
- **Users with Purchases**: 4,419 (1.64%)
- **Repeat Buyers**: 775 (17.5% of buyers)
- **User Segments**:
  - Active (Last 7 days)
  - Recent (Last 30 days)
  - Dormant (Last 90 days)
  - Inactive (90+ days)

## 🔍 Sample Queries

### Top Converting Traffic Sources
```sql
SELECT
  traffic_source_full,
  total_sessions,
  sessions_with_purchase,
  session_overall_conversion_rate,
  users_with_purchase
FROM `ra-warehouse-dev.analytics_ga4_marts.conversion_funnel`
WHERE event_date = '2021-01-31'
ORDER BY sessions_with_purchase DESC
LIMIT 10;
```

### High-Value Customers
```sql
SELECT
  user_pseudo_id,
  total_revenue,
  total_purchases,
  average_order_value,
  user_type,
  monetary_segment,
  recency_segment
FROM `ra-warehouse-dev.analytics_ga4_marts.users`
WHERE total_revenue > 0
ORDER BY total_revenue DESC
LIMIT 100;
```

### Product Performance
```sql
WITH product_views AS (
  SELECT item_id, item_name, COUNT(*) as views
  FROM `ra-warehouse-dev.analytics_ga4_marts.product_views`
  GROUP BY item_id, item_name
),
add_to_carts AS (
  SELECT item_id, COUNT(*) as carts
  FROM `ra-warehouse-dev.analytics_ga4_marts.add_to_cart_events`
  GROUP BY item_id
),
purchases AS (
  SELECT item_id, COUNT(*) as purchases, SUM(item_revenue_usd) as revenue
  FROM `ra-warehouse-dev.analytics_ga4_marts.purchase_events`
  GROUP BY item_id
)
SELECT
  pv.item_name,
  pv.views,
  COALESCE(ac.carts, 0) as add_to_carts,
  COALESCE(p.purchases, 0) as purchases,
  COALESCE(p.revenue, 0) as revenue,
  SAFE_DIVIDE(COALESCE(p.purchases, 0), pv.views) as view_to_purchase_rate
FROM product_views pv
LEFT JOIN add_to_carts ac ON pv.item_id = ac.item_id
LEFT JOIN purchases p ON pv.item_id = p.item_id
ORDER BY views DESC
LIMIT 20;
```

## 📝 Notes

- **Source Data**: BigQuery Public Data - GA4 Obfuscated Sample E-commerce
- **Date Range**: November 2020 - January 2021
- **Database**: ra-warehouse-dev
- **Schema**: analytics_ga4_marts
- **dbt Version**: 1.11.0-b4
- **Adapter**: dbt-bigquery 1.10.3

## 🤝 Contributing

To add new models or modify existing ones:
1. Update the SQL in `models/marts/`
2. Add documentation in `models/marts/schema.yml`
3. Add tests as appropriate
4. Run `dbt run` and `dbt test` to verify
5. Update this README with any new metrics or models
