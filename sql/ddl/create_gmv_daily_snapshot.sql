-- DDL: daily_snapshot
CREATE TABLE gmv_daily_snapshot (
    -- Keys
    snapshot_date DATE NOT NULL,
    purchase_id BIGINT NOT NULL,
    
    -- purchase data
    buyer_id BIGINT,
    producer_id BIGINT,
    order_date DATE,
    release_date DATE,
    
    -- product_item data
    product_id BIGINT,
    item_quantity INT,
    purchase_value DECIMAL(18,2),
    
    -- purchase_extra_info data
    subsidiary VARCHAR(50),
    
    -- Flags
    is_current BOOLEAN,
    is_paid BOOLEAN,
    
    -- Partition
    transaction_date DATE NOT NULL,
    
    PRIMARY KEY (snapshot_date, purchase_id)
)
PARTITION BY (transaction_date);

-- Indexes
CREATE INDEX idx_gmv_current ON gmv_daily_snapshot(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_gmv_subsidiary ON gmv_daily_snapshot(subsidiary);
CREATE INDEX idx_gmv_snapshot_date ON gmv_daily_snapshot(snapshot_date);