# GA4 E-commerce Analytics - dbt Package

A comprehensive dbt package for transforming Google Analytics 4, Snowplow, and ContentSquare e-commerce and behavioral data into analytics-ready models with web analytics, e-commerce, UX metrics, and conversion rate analytics.

## Multi-Source Support

This package supports **unified analytics across multiple tracking sources**:

- **GA4**: Google Analytics 4 e-commerce events
- **Snowplow**: Snowplow event tracking with GA4 Ecommerce Adapter
- **ContentSquare**: Digital experience analytics with UX metrics and frustration signals

All sources are seamlessly integrated through a three-tier architecture:
1. **Staging Layer**: Source-specific transformations
2. **Integration Layer**: Unified schemas with source identification
3. **Warehouse Layer**: Analytics-ready fact tables and dimensions

### Snowplow with GA4 Adapter

When using Snowplow with the **GA4 Ecommerce Adapter**, events sent through Google Tag Manager are captured by both GA4 and Snowplow simultaneously. This package automatically:

- Maps Snowplow event schemas to GA4-compatible structures
- Unions data from both sources with a common `source` column
- Preserves Snowplow-specific enrichments (browser details, enhanced geography, user properties)
- Handles schema differences in ecommerce items arrays
- Supports flexible source enabling/disabling via configuration

**Key Benefits:**
- **Data Redundancy**: Compare metrics across tracking systems
- **Enhanced Data**: Access Snowplow's additional enrichments alongside GA4 data
- **Flexible Analysis**: Analyze by source or combined metrics
- **Migration Path**: Easy transition between tracking platforms

### ContentSquare Digital Experience Analytics

**ContentSquare** provides specialized behavioral analytics and UX insights that complement traditional web analytics. This package automatically:

- Integrates ContentSquare sessions and pageviews with GA4/Snowplow data
- Preserves ContentSquare-specific **Web Vitals** metrics (FID, LCP, CLS, INP, TTFB)
- Captures **frustration signals** (rage clicks, excessive hovering, looping)
- Tracks **JavaScript errors** and **API errors** for debugging
- Provides **UX scoring** (frustration score, page consumption, looping index)
- Enables cross-source behavioral analysis with unified session/user IDs

**Key Differences from GA4/Snowplow:**
- **Pre-aggregated sessions**: ContentSquare provides session-level data directly
- **No item-level ecommerce**: Transactions tracked at total level only (no items array)
- **Rich UX metrics**: Built-in frustration scoring and engagement metrics
- **Error tracking**: Comprehensive error and performance issue capture

**ContentSquare-Specific Fact Tables:**
- `rage_click_fct`: Frustration events (rage clicks, excessive hovering)
- `js_error_fct`: JavaScript error events with stack traces
- `user_interaction_fct`: User interactions (add to cart, clicks) without item details

**Integrated Tables (ContentSquare + GA4 + Snowplow):**
- `session_fct`: Sessions with ContentSquare frustration scoring
- `pageview_fct`: Pageviews with ContentSquare Web Vitals

## Models

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

## Configuration

### Enabling/Disabling Data Sources

Control which tracking sources are included in your analytics via `dbt_project.yml`:

```yaml
vars:
  enable_ga4_source: true          # Enable/disable GA4 data
  enable_snowplow_source: true      # Enable/disable Snowplow data
  enable_contentsquare_source: true # Enable/disable ContentSquare data
```

**Configuration Options:**
- **All enabled** (recommended): Union data from all sources for complete analytics coverage
- **GA4 only**: `enable_ga4_source: true`, others `false`
- **Snowplow only**: `enable_snowplow_source: true`, others `false`
- **ContentSquare only**: `enable_contentsquare_source: true`, others `false`
- **GA4 + Snowplow**: Both `true`, `enable_contentsquare_source: false`
- **GA4 + ContentSquare**: `enable_ga4_source: true`, `enable_contentsquare_source: true`

When multiple sources are enabled, data is automatically unioned in the integration layer with a `source` column identifying the origin (ga4/snowplow/contentsquare).

**Note**: ContentSquare integration includes:
- Sessions and pageviews unified with GA4/Snowplow
- ContentSquare-specific behavioral fact tables (rage_click_fct, js_error_fct, user_interaction_fct)
- Web Vitals and UX metrics available in integrated models

## Project Structure

```
ra_ga4_ecomm/
├── models/
│   ├── staging/
│   │   ├── ga4/
│   │   │   ├── stg_ga4__event.sql          # GA4 staging model
│   │   │   └── stg_ga4.yml                 # GA4 tests & documentation
│   │   ├── snowplow/
│   │   │   ├── stg_snowplow__event.sql     # Snowplow staging model
│   │   │   └── stg_snowplow.yml            # Snowplow tests & documentation
│   │   └── contentsquare/
│   │       ├── stg_contentsquare__pageview.sql      # CS pageview staging
│   │       ├── stg_contentsquare__add_to_cart.sql   # CS add to cart staging
│   │       ├── stg_contentsquare__purchase.sql      # CS purchase staging
│   │       ├── stg_contentsquare__rage_click.sql    # CS rage click staging
│   │       ├── stg_contentsquare__js_error.sql      # CS error staging
│   │       └── stg_contentsquare.yml                # CS tests & documentation
│   ├── integration/
│   │   ├── int__session.sql                # Unified session data (GA4 + Snowplow + CS)
│   │   ├── int__pageview.sql               # Unified pageview data (GA4 + Snowplow + CS)
│   │   ├── int__add_to_cart.sql            # Unified cart events (GA4 + Snowplow)
│   │   ├── int__purchase.sql               # Unified purchase events (GA4 + Snowplow)
│   │   ├── int__product_view.sql           # Unified product views (GA4 + Snowplow)
│   │   └── integration.yml                 # Integration tests & documentation
│   ├── warehouse/
│   │   ├── web_analytics/                  # Web analytics domain
│   │   │   ├── session_fct.sql             # Session fact table (multi-source)
│   │   │   ├── pageview_fct.sql            # Pageview fact table (multi-source)
│   │   │   └── web_analytics.yml           # Web analytics tests & documentation
│   │   ├── ecommerce/                      # E-commerce domain
│   │   │   ├── add_to_cart_fct.sql         # Add to cart fact table
│   │   │   ├── purchase_fct.sql            # Purchase fact table
│   │   │   ├── product_view_fct.sql        # Product view fact table
│   │   │   ├── user_dim.sql                # User dimension
│   │   │   ├── conversion_funnel_fct.sql   # Conversion funnel
│   │   │   └── ecommerce.yml               # E-commerce tests & documentation
│   │   └── digital_experience/             # Digital experience domain (ContentSquare)
│   │       ├── rage_click_fct.sql          # Rage click frustration events
│   │       ├── js_error_fct.sql            # JavaScript error events
│   │       ├── user_interaction_fct.sql    # User interaction events
│   │       └── digital_experience.yml      # DX tests & documentation
│   ├── marts/
│   │   ├── sessions.sql                    # Session mart (view)
│   │   ├── pageviews.sql                   # Pageview mart (view)
│   │   ├── users.sql                       # User mart (view)
│   │   ├── add_to_cart_events.sql          # Cart events mart (view)
│   │   ├── purchase_events.sql             # Purchase mart (view)
│   │   ├── product_views.sql               # Product views mart (view)
│   │   ├── conversion_funnel.sql           # Funnel mart (view)
│   │   └── schema.yml                      # Mart documentation
│   └── sources.yml                         # Source definitions
└── analyses/
    └── data_quality_validation.sql         # Data quality checks
```

### Model Layer Architecture

**Staging Layer (`staging/`):**
- Source-specific transformations
- Field renaming and type casting
- Basic data cleaning
- Conditionally enabled via variables

**Integration Layer (`integration/`):**
- Unions data from multiple sources
- Adds `source` column for identification
- Provides superset schema (all fields from all sources)
- Maps source-specific fields to common schema
- Handles items array schema normalization

**Warehouse Layer (`warehouse/`):**
- Organized by business domain (web_analytics, ecommerce, digital_experience)
- Generates surrogate keys (including source)
- Applies business logic and calculations
- Incremental materialization with partitioning
- Includes all source-specific enrichments
- **web_analytics/**: Multi-source session and pageview facts
- **ecommerce/**: Transaction and product analytics
- **digital_experience/**: UX metrics and behavioral signals (ContentSquare)

**Marts Layer (`marts/`):**
- Final user-facing views
- Simplified column names
- Documentation and examples

## Quick Start

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

### Validation Results (Latest - Multi-Source)

**Data Volume by Source:**

| Metric | GA4 | Snowplow | Total |
|--------|-----|----------|-------|
| Sessions | 360,974 | 91 | 361,065 |
| Page Views | 1,350,428 | 34 | 1,350,462 |
| Add to Cart | 667,426 | 37 | 667,463 |
| Purchases | 16,003 | 12 | 16,015 |
| Product Views | 2,748,246 | 53 | 2,748,299 |

**Quality Metrics:**
- **361K** sessions aggregated from both sources
- **270K** unique users identified
- **1.35M** page views processed with navigation flow
- **667K** add-to-cart events tracked
- **2.75M** product views recorded
- **16K** purchase items processed
- **$362K** total revenue tracked
- **4,419** users with purchases
- **775** repeat buyers
- **86/86** data quality tests passing (100% pass rate)

### Known Data Characteristics
- Some purchase events may have null `transaction_id` (59 out of 16K) - this is present in the source data
- Funnel progressions may not be strictly hierarchical (users can skip steps in GA4 e-commerce tracking)
- Small rounding differences (~$55 out of $362K) between revenue aggregations are expected

## Snowplow-Specific Enrichments

When Snowplow data is enabled, the following additional fields are available in all warehouse fact tables:

### Browser Enrichments
- `browser_family` - Browser family (e.g., Chrome, Firefox, Safari)
- `browser_name` - Specific browser name and version
- `browser_version` - Browser version number
- `browser_viewheight` - Viewport height in pixels
- `browser_viewwidth` - Viewport width in pixels

### Device Enrichments
- `device_type` - Device type (desktop, mobile, tablet)
- `device_is_mobile` - Boolean flag for mobile devices
- `device_screen_height` - Screen height in pixels
- `device_screen_width` - Screen width in pixels

### Enhanced Geography
- `geo_region_name` - Full region/state name (not just code)
- `geo_zipcode` - Postal/ZIP code
- `geo_latitude` - Latitude coordinates
- `geo_longitude` - Longitude coordinates
- `geo_timezone` - Local timezone

### User Properties (Custom)
- `user_customer_segment` - Customer segmentation data
- `user_loyalty_tier` - Loyalty program tier
- `user_subscription_status` - Subscription status

### Additional Traffic Source Fields
- `traffic_source_content` - Campaign content parameter
- `traffic_source_term` - Campaign term/keyword parameter

### Ecommerce Enhancements
- `ecommerce_value` - Transaction value in original currency
- `ecommerce_currency` - Transaction currency code
- `discount` - Item-level discount amount (in items array)
- `index` - Item position in list (in items array)

**Usage Example:**
```sql
SELECT
  source,
  device_category,
  browser_family,
  device_is_mobile,
  geo_zipcode,
  COUNT(*) as sessions
FROM `ra-warehouse-dev.analytics_ga4.session_fct`
WHERE device_is_mobile IS NOT NULL  -- Snowplow sessions only
GROUP BY 1, 2, 3, 4, 5
ORDER BY sessions DESC;
```

## ContentSquare-Specific Enrichments

When ContentSquare data is enabled, the following additional fields are available:

### Web Vitals (Performance Metrics) - `pageview_fct`
**Core Web Vitals:**
- `first_input_delay` (FID) - Time from user interaction to browser response (ms)
- `largest_contentful_paint` (LCP) - Largest content render time (ms)
- `cumulative_layout_shift` (CLS) - Visual stability score
- `interaction_to_next_paint` (INP) - Interaction responsiveness (ms)

**Additional Performance Metrics:**
- `time_to_first_byte` (TTFB) - Server response time (ms)
- `first_contentful_paint` (FCP) - First content render time (ms)
- `dom_interactive_after_msec` - DOM ready time (ms)
- `fully_loaded` - Complete page load time (ms)
- `start_render` - Initial render time (ms)

**Engagement Metrics:**
- `scroll_rate` - Percentage of page scrolled (0-100)
- `view_duration_msec` - Time spent on page (ms)
- `window_height` - Browser window height (px)
- `window_width` - Browser window width (px)

### UX & Frustration Metrics - `session_fct`
- `frustration_score` - Composite frustration score (0-100)
- `looping_index` - User navigation looping behavior
- `page_consumption` - Content consumption score

### Behavioral Event Tables

**`rage_click_fct` - Frustration Events:**
- `target_text` - Text of clicked element
- `target_path` - DOM path to element
- `frustration_score` - Event-level frustration score
- `relative_time` - Time into session when occurred
- `click_count` - Number of rapid clicks

**`js_error_fct` - JavaScript Errors:**
- `error_message` - Error message text
- `error_file_name` - File where error occurred
- `error_line_number` - Line number of error
- `error_column_number` - Column number of error
- `errors_after_clicks` - Errors triggered by user action
- `error_group_id` - Error grouping identifier
- `error_source` - Error source type

**`user_interaction_fct` - User Interactions:**
- `target_text` - Interaction target element text
- `href` - Link URL if applicable
- `event_type` - Type of interaction (add_to_cart, etc.)
- All standard traffic source and device fields

**Usage Examples:**

Analyze page performance issues:
```sql
SELECT
  page_path,
  AVG(largest_contentful_paint) as avg_lcp,
  AVG(cumulative_layout_shift) as avg_cls,
  AVG(first_input_delay) as avg_fid,
  COUNT(*) as pageviews
FROM `ra-warehouse-dev.analytics_ga4.pageview_fct`
WHERE source = 'contentsquare'
  AND largest_contentful_paint IS NOT NULL
GROUP BY 1
HAVING avg_lcp > 2500  -- LCP > 2.5s is poor
ORDER BY avg_lcp DESC;
```

Identify frustration patterns:
```sql
SELECT
  user_fk,
  COUNT(DISTINCT ga_session_id) as sessions_with_rage,
  AVG(frustration_score) as avg_frustration,
  COUNT(*) as rage_click_events
FROM `ra-warehouse-dev.analytics_ga4.rage_click_fct`
GROUP BY 1
HAVING rage_click_events > 5
ORDER BY avg_frustration DESC;
```

Correlate errors with frustration:
```sql
SELECT
  s.frustration_score,
  COUNT(DISTINCT e.js_error_pk) as js_errors,
  COUNT(DISTINCT r.rage_click_pk) as rage_clicks
FROM `ra-warehouse-dev.analytics_ga4.session_fct` s
LEFT JOIN `ra-warehouse-dev.analytics_ga4.js_error_fct` e
  ON s.ga_session_id = e.ga_session_id
LEFT JOIN `ra-warehouse-dev.analytics_ga4.rage_click_fct` r
  ON s.ga_session_id = r.ga_session_id
WHERE s.source = 'contentsquare'
GROUP BY 1
ORDER BY 1;
```

## Incremental Loading

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

## Key Business Metrics

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

### Comparing Metrics Across Sources

Analyze discrepancies or validate tracking implementation:

```sql
SELECT
  event_date,
  source,
  COUNT(DISTINCT user_fk) as unique_users,
  COUNT(*) as total_sessions,
  SUM(page_views_per_session) as total_pageviews,
  SUM(purchase_events_per_session) as total_purchases,
  SUM(purchase_revenue_per_session) as total_revenue,
  SAFE_DIVIDE(SUM(purchase_revenue_per_session), COUNT(*)) as revenue_per_session
FROM `ra-warehouse-dev.analytics_ga4.session_fct`
WHERE event_date BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY event_date, source
ORDER BY event_date, source;
```

### Analyze Snowplow Enrichments

Leverage Snowplow-specific data for deeper insights:

```sql
WITH snowplow_sessions AS (
  SELECT
    user_fk,
    browser_family,
    device_type,
    geo_zipcode,
    geo_timezone,
    user_customer_segment,
    user_loyalty_tier,
    session_duration_seconds,
    purchase_revenue_per_session
  FROM `ra-warehouse-dev.analytics_ga4.session_fct`
  WHERE source = 'snowplow'
    AND event_date >= '2021-01-01'
)
SELECT
  browser_family,
  user_loyalty_tier,
  COUNT(*) as sessions,
  AVG(session_duration_seconds) as avg_duration,
  SUM(purchase_revenue_per_session) as revenue,
  COUNT(DISTINCT user_fk) as unique_users
FROM snowplow_sessions
WHERE user_loyalty_tier IS NOT NULL
GROUP BY browser_family, user_loyalty_tier
ORDER BY revenue DESC;
```

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

### Data Sources

**GA4 Source:**
- **Source Data**: BigQuery Public Data - GA4 Obfuscated Sample E-commerce
- **Date Range**: November 2020 - January 2021
- **Table**: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

**Snowplow Source:**
- **Source Data**: Snowplow events with GA4 Ecommerce Adapter
- **Sample Events**: 100 sample events
- **Table**: `ra-warehouse-dev.snowplow_ecommerce_sample.events`
- **Adapter**: GA4 Ecommerce Adapter (sends same events as GA4 via GTM)

### Technical Details

- **Database**: ra-warehouse-dev
- **Schemas**:
  - `analytics_ga4_staging` - Staging models
  - `analytics_ga4_integration` - Integration models
  - `analytics_ga4` - Warehouse models
  - `analytics_ga4_marts` - Mart views
- **dbt Version**: 1.11.0-b4
- **Adapter**: dbt-bigquery 1.10.3

### Testing Coverage

- **Total Tests**: 86
- **Staging Tests**: 7 (Snowplow) + existing GA4 tests
- **Integration Tests**: 41 (covering all integration models)
- **Warehouse Tests**: 38 (covering all fact tables and dimensions)
- **Pass Rate**: 100%

## Technical Implementation: Snowplow Integration

### How the Integration Works

When Snowplow is configured with the **GA4 Ecommerce Adapter**, events from Google Tag Manager are sent to both GA4 and Snowplow simultaneously. This package handles the integration through:

#### 1. **Schema Mapping** (`stg_snowplow__event`)
- Extracts Snowplow events from the source table
- Maps Snowplow field names to GA4 equivalents
- Extracts `items` array from `ecommerce` struct
- Casts session_id to string for consistency with GA4

#### 2. **Items Array Normalization** (`int__*` models)
Snowplow and GA4 have slightly different items array schemas. The integration layer uses explicit type casting to create a unified schema:

```sql
array(
  select as struct
    cast(item.item_id as string) as item_id,
    cast(item.item_name as string) as item_name,
    -- ... 26 total fields with explicit types
    cast(item.discount as float64) as discount,  -- Snowplow-specific
    cast(null as float64) as discount  -- NULL for GA4
  from unnest(items) as item
) as items
```

This ensures:
- Successful UNION ALL operations
- Consistent data types across sources
- Preservation of source-specific fields
- NULL handling for missing fields

#### 3. **Conditional Source Loading**

Integration models use Jinja templating for flexible source inclusion:

```sql
{% if var('enable_ga4_source', true) %}
  -- GA4 CTE
{% endif %}

{% if var('enable_snowplow_source', false) %}
  -- Snowplow CTE
{% endif %}

final as (
  {% if var('enable_ga4_source', true) %}
    select * from ga4_[entity]
  {% endif %}

  {% if var('enable_ga4_source', true) and var('enable_snowplow_source', false) %}
    union all
  {% endif %}

  {% if var('enable_snowplow_source', false) %}
    select * from snowplow_[entity]
  {% endif %}
)
```

#### 4. **Surrogate Key Generation**

Warehouse models include `source` in surrogate keys to ensure uniqueness:

```sql
{{ dbt_utils.generate_surrogate_key([
    'source',
    'event_date',
    'user_fk',
    'ga_session_id'
]) }} as session_pk
```

This prevents collisions if the same event exists in both sources.

### Field Mapping Reference

| Snowplow Field | GA4 Equivalent | Notes |
|----------------|----------------|-------|
| `session_id` | `ga_session_id` | Cast to string |
| `ecommerce.value` | `purchase_revenue` | Transaction total |
| `ecommerce.tax` | `tax_value_in_usd` | Tax amount |
| `ecommerce.shipping` | `shipping_value_in_usd` | Shipping cost |
| `ecommerce.currency` | `ecommerce_currency` | Snowplow-specific |
| `items[].discount` | N/A | Snowplow-specific |
| `items[].index` | `item_list_index` | List position |
| `geo.latitude/longitude` | N/A | Snowplow-specific |
| `user_properties.*` | N/A | Snowplow-specific |

### Adding New Sources

To add additional tracking sources:

1. Create staging model in `models/staging/[source]/`
2. Map source schema to common field names
3. Update integration models to include new source CTE
4. Add source variable to `dbt_project.yml`
5. Update warehouse model surrogate keys
6. Add tests in `[source].yml`

## 🤝 Contributing

To add new models or modify existing ones:
1. Update the SQL in appropriate layer (`staging/`, `integration/`, `warehouse/`, `marts/`)
2. Add documentation in layer-specific `.yml` files
3. Add tests following existing patterns
4. Run `dbt run` and `dbt test` to verify
5. Update this README with any new metrics or models
6. For new sources, follow the "Adding New Sources" guide above
