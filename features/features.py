from datetime import datetime
from pathlib import Path

from feast import Entity, Feature, FeatureView, ValueType
from feast.data_source import FileSource
from google.protobuf.duration_pb2 import Duration


# Read data
START_TIME = "2022-01-01"
project_details = FileSource(
    path="/Users/hunr/PycharmProjects/feastdemo/data/features.parquet",
    event_timestamp_column="created_on",
)

# Define an entity for the project
project = Entity(
    name="store_sk_id",
    value_type=ValueType.INT64,
    description="project id",
)

# Define a Feature View for each project
# Can be used for fetching historical data and online serving
project_details_view = FeatureView(
    name="store_details",
    entities=["store_sk_id"],
    ttl=Duration(
        seconds=(datetime.today() - datetime.strptime(START_TIME, "%Y-%m-%d")).days * 24 * 60 * 60
    ),
    features=[
        Feature(name="current_ind", dtype=ValueType.STRING),
        Feature(name="store_nbr", dtype=ValueType.INT64),
        Feature(name="store_name", dtype=ValueType.STRING),
        Feature(name="street_addr_line1", dtype=ValueType.STRING),
        Feature(name="city_name", dtype=ValueType.STRING),
        Feature(name="state_prov_code", dtype=ValueType.STRING),
        Feature(name="postal_code", dtype=ValueType.STRING),
        Feature(name="subdiv_nbr", dtype=ValueType.STRING),
        Feature(name="subdiv_name", dtype=ValueType.STRING),
        Feature(name="region_name", dtype=ValueType.STRING),
        Feature(name="region_nbr", dtype=ValueType.INT64),
        Feature(name="market_area_nbr", dtype=ValueType.INT64),
        Feature(name="market_area_name", dtype=ValueType.STRING),
        Feature(name="primary_trait_name", dtype=ValueType.STRING),
        Feature(name="county_name", dtype=ValueType.STRING),
        Feature(name="phone_nbr", dtype=ValueType.STRING),
        Feature(name="store_size_qty", dtype=ValueType.INT64),
        Feature(name="store_size_uom_code", dtype=ValueType.STRING),
        Feature(name="store_size_uom_desc", dtype=ValueType.STRING),
        Feature(name="open_sunday_ind", dtype=ValueType.STRING),
        Feature(name="open_status_code", dtype=ValueType.STRING),
        Feature(name="open_status_desc", dtype=ValueType.STRING),
        Feature(name="expansion_size_qty", dtype=ValueType.INT64),
        Feature(name="real_store_nbr", dtype=ValueType.INT64),
        Feature(name="temp_store_nbr", dtype=ValueType.INT64),
        Feature(name="temp_store_ind", dtype=ValueType.STRING),
        Feature(name="store_type_code", dtype=ValueType.STRING),
        Feature(name="store_type_desc", dtype=ValueType.STRING),
        Feature(name="size_class_code", dtype=ValueType.INT64),
        Feature(name="sales_class_code", dtype=ValueType.INT64),
        Feature(name="store_comp_code", dtype=ValueType.STRING),
        Feature(name="store_comp_desc", dtype=ValueType.STRING),
        Feature(name="store_comp_ind", dtype=ValueType.STRING),
        Feature(name="financial_rpt_code", dtype=ValueType.STRING),
        Feature(name="financial_rpt_desc", dtype=ValueType.STRING),
        Feature(name="delivery_type_code", dtype=ValueType.STRING),
        Feature(name="delivery_type_desc", dtype=ValueType.STRING),
        Feature(name="prototype_nbr", dtype=ValueType.STRING),
        Feature(name="time_zone_code", dtype=ValueType.STRING),
        Feature(name="latitude_dgr", dtype=ValueType.FLOAT),
        Feature(name="longitude_dgr", dtype=ValueType.FLOAT),
        Feature(name="apparel_zone_nbr", dtype=ValueType.INT64),
        Feature(name="mdse_major_zone_nbr", dtype=ValueType.INT64),
        Feature(name="mdse_sub_zone_nbr", dtype=ValueType.INT64),
        Feature(name="banner_code", dtype=ValueType.STRING),
        Feature(name="banner_desc", dtype=ValueType.STRING),
        Feature(name="store_mgr_name", dtype=ValueType.STRING),
        Feature(name="buo_area_nbr", dtype=ValueType.INT64),
        Feature(name="base_div_nbr", dtype=ValueType.INT64),
        Feature(name="buo_area_name", dtype=ValueType.STRING),
        Feature(name="subregion_nbr", dtype=ValueType.STRING),
        Feature(name="subregion_name", dtype=ValueType.STRING),
        Feature(name="dec_store_nbr", dtype=ValueType.STRING),
        Feature(name="buo_area_mgr_name", dtype=ValueType.STRING),
        Feature(name="subdiv_mgr_name", dtype=ValueType.STRING),
        Feature(name="region_mgr_name", dtype=ValueType.STRING),
        Feature(name="subregion_mgr_name", dtype=ValueType.STRING),
        Feature(name="market_area_mgr_name", dtype=ValueType.STRING),
        Feature(name="geo_point_nbr", dtype=ValueType.STRING)
    ],
    online=True,
    input=project_details,
    tags={},
)
