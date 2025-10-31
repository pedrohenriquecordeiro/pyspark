from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
    LongType
)

schemas = {

    'corp-app-prod.main_booking.bookings': {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField("created_at", LongType(), True),
            StructField('id', LongType(), False),
            StructField('old_id', LongType(), True),
            StructField('organization_id', StringType(), True),
            StructField('origin_id', StringType(), False),
            StructField('protocol', StringType(), True),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True),
            StructField('user_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.buses' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('booking_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', LongType(), False),
            StructField('old_id', LongType(), True),
            StructField('origin_id', StringType(), False),
            StructField('status', StringType(), True),
            StructField('trip_type', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_additional_charge_refunds' : {
        "primary_keys": 'bus_additional_charge_id',
        "schema" : StructType([
            StructField('bus_additional_charge_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.bus_additional_charges' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), False),
            StructField('id', StringType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_fare_refund_policies' : {
        "primary_keys": ['bus_fare_id'],
        "schema" : StructType([
            StructField('bus_fare_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('end', LongType(), False),
            StructField('fee', LongType(), False),
            StructField('id', StringType(), False),
            StructField('start', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_fare_services' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_fare_id', StringType(), False),
            StructField('bus_service_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_fare_taxes' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_fare_id', StringType(), False),
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_fares' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_leg_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('seat_class', StringType(), False),
            StructField('travel_entity_id', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_histories' : {
        "primary_keys": ['history_id' , 'bus_id'],
        "schema" : StructType([
            StructField('bus_id', LongType(), False),
            StructField('history_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.bus_leg_histories' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_leg_id', StringType(), False),
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.bus_legs' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('arrival_date', LongType(), False),
            StructField('arrival_station', StringType(), False),
            StructField('bus_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('departure_date', LongType(), False),
            StructField('departure_station', StringType(), False),
            StructField('duration', LongType(), False),
            StructField('id', StringType(), False),
            StructField('operating_company', StringType(), False),
            StructField('step', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_passenger_additional_services' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_passenger_id', StringType(), False),
            StructField('bus_service_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), True),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_passenger_refunds' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bus_id', LongType(), False),
            StructField('bus_leg_id', StringType(), False),
            StructField('bus_passenger_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.bus_passenger_seats' : {
        "primary_keys":  ['id'],
        "schema" : StructType([
            StructField('bpe', StringType(), True),
            StructField('bus_leg_id', StringType(), False),
            StructField('bus_passenger_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('seat_number', StringType(), False),
            StructField('ticket_number', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_passengers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('bus_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.bus_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('eligible', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.credits': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('amount', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('credit_id', StringType(), False),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('is_cancelled', LongType(), False),
            StructField('refund_id', StringType(), True),
            StructField('type', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.customer_access_entities': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('access_entity_id', StringType(), False),
            StructField('access_entity_type', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.customer_documents': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('document_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.customer_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('customer_id', StringType(), False),
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.customers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('birthday', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('email', StringType(), True),
            StructField('first_name', StringType(), False),
            StructField('gender', StringType(), False),
            StructField('id', StringType(), False),
            StructField('last_name', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.document_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('document_id', StringType(), False),
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.documents': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('deleted_at', LongType(), True),
            StructField('document_type', StringType(), True),
            StructField('document_value', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.duplicated_buses': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_protocols_array', StringType(), False),
            StructField('bus_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.duplicated_flights': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_protocols_array', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.duplicated_hotels': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_protocols_array', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.duplicated_rental_cars': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_protocols_array', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_additional_charge_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('flight_additional_charge_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_additional_charges': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), False),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_fare_refund_policies': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('end', LongType(), False),
            StructField('fee', LongType(), False),
            StructField('flight_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('start', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_fare_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('flight_fare_id', StringType(), False),
            StructField('flight_service_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_fare_taxes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('adult_price', LongType(), True),
            StructField('children_price', LongType(), True),
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('flight_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('infant_price', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_fares': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('adult_price', LongType(), True),
            StructField('adult_reference_price', LongType(), True),
            StructField('business_class', StringType(), False),
            StructField('carbon_emission', LongType(), True),
            StructField('children_price', LongType(), True),
            StructField('children_reference_price', LongType(), True),
            StructField("created_at", LongType(), True),
            StructField('fare_family', StringType(), False),
            StructField('id', StringType(), False),
            StructField('infant_price', LongType(), True),
            StructField('infant_reference_price', LongType(), True),
            StructField('travel_entity_id', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_histories': {
        "primary_keys": ['flight_id', 'history_id'],
        "schema": StructType([
            StructField('flight_id', LongType(), False),
            StructField('history_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_requotes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('accepted_by_user_id', StringType(), True),
            StructField("created_at", LongType(), True),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('requote_failed', LongType(), False),
            StructField('token', StringType(), True),
            StructField('total_from', LongType(), True),
            StructField('total_to', LongType(), True),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_notified', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_segment_leg_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('flight_segment_leg_id', StringType(), False),
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_segment_legs': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('arrival_airport', StringType(), False),
            StructField('arrival_date', LongType(), False),
            StructField('cia_locator', StringType(), True),
            StructField("created_at", LongType(), True),
            StructField('departure_airport', StringType(), False),
            StructField('departure_date', LongType(), False),
            StructField('duration', LongType(), False),
            StructField('flight_number', StringType(), False),
            StructField('flight_segment_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('layover', LongType(), True),
            StructField('managing_airline', StringType(), False),
            StructField('operating_airline', StringType(), False),
            StructField('step', LongType(), False),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_segments': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('flight_fare_id', StringType(), False),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('segment', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('available_price', LongType(), True),
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('due_date', LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), True),
            StructField('quantity', LongType(), True),
            StructField('updated_at', LongType(), True),
            StructField('value', StringType(), True),
            StructField('value_type', StringType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_traveler_additional_service_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('flight_traveler_additional_service_id', StringType(), False),
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.flight_traveler_additional_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('flight_segment_id', StringType(), False),
            StructField('flight_service_id', StringType(), False),
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('price', LongType(), True),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_traveler_booking_locators': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_locator', StringType(), True),
            StructField("created_at", LongType(), True),
            StructField('flight_segment_id', StringType(), False),
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('related_cia_locator', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_traveler_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('flight_id', LongType(), False),
            StructField('flight_segment_id', StringType(), False),
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('refund_id', StringType(), False),
            StructField('replaced_by_flight_traveler_refund_id', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flight_travelers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('flight_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.flights': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', LongType(), False),
            StructField('is_international', IntegerType(), False),
            StructField('is_package', LongType(), False),
            StructField('old_id', LongType(), True),
            StructField('origin_id', StringType(), False),
            StructField('split_at', LongType(), True),
            StructField('status', StringType(), True),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('action', StringType(), True),
            StructField("created_at", LongType(), True),
            StructField('executor_id', StringType(), False),
            StructField('executor_type', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.history_changes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('field', StringType(), False),
            StructField('from', StringType(), True),
            StructField('history_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('to', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_additional_charge_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('hotel_additional_charge_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_additional_charges': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), False),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_amenities': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('label', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_booking_locators': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_locator', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('room_index', LongType(), False),
            StructField('room_locator', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_fare_meals': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('breakfast', LongType(), True),
            StructField("created_at", LongType(), True),
            StructField('dinner', LongType(), True),
            StructField('hotel_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('lunch', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_fare_refund_policies': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('end', LongType(), False),
            StructField('fee', LongType(), True),
            StructField('hotel_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('start', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_fare_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('hotel_fare_id', StringType(), False),
            StructField('hotel_service_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_fare_taxes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('hotel_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_fares': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('hotel_room_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('is_self_checkin', LongType(), False),
            StructField('price', LongType(), False),
            StructField('reference_price', LongType(), True),
            StructField('travel_entity_id', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_guest_additional_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('hotel_guest_id', StringType(), False),
            StructField('hotel_service_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('price', LongType(), True),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_guests': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('hotel_fare_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_histories': {
        "primary_keys": ['hotel_id', 'history_id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('hotel_id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_location_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('hotel_location_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_locations': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('city'      , StringType(), False),
            StructField('country'   , StringType(), False),
            StructField("created_at", LongType()  , True),
            StructField('district'  , StringType(), True),
            StructField('hotel_id'  , LongType()  , False),
            StructField('id'        , StringType(), False),
            StructField('lat'       , DoubleType(), True),
            StructField('lng'       , DoubleType(), True),
            StructField('number'    , StringType(), True),
            StructField('place_id'  , StringType(), True),
            StructField('state'     , StringType(), True),
            StructField('street'    , StringType(), True),
            StructField('updated_at', LongType()  , True)
        ])
    },

    'corp-app-prod.main_booking.hotel_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('hotel_id', LongType(), False),
            StructField('id', LongType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_requotes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('accepted_by_user_id', StringType(), True),
            StructField("created_at", LongType(), True),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('requote_failed', LongType(), False),
            StructField('token', StringType(), True),
            StructField('total_from', LongType(), True),
            StructField('total_to', LongType(), True),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('user_notified', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_room_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('hotel_room_id', StringType(), False),
            StructField('id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotel_rooms': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('accommodation', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('done_self_check_in', LongType(), True),
            StructField('hotel_id', LongType(), False),
            StructField('id', StringType(), False),
            StructField('index', LongType(), False),
            StructField('self_check_in_url', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.hotel_services': { 
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('eligible', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.hotels': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('address', StringType(), False),
            StructField('booking_id', LongType(), False),
            StructField('check_in', LongType(), False),
            StructField('check_in_hour', LongType(), False),
            StructField('check_out', LongType(), False),
            StructField('check_out_hour', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), True),
            StructField('id', LongType(), False),
            StructField('images_url', StringType(), True),
            StructField('is_international', IntegerType(), False),
            StructField('is_short_stays', LongType(), False),
            StructField('name', StringType(), False),
            StructField('old_id', LongType(), True),
            StructField('origin_id', StringType(), False),
            StructField('poi_place_id', StringType(), True),
            StructField('stars', LongType(), False),
            StructField('status', StringType(), True),
            StructField('thumb_url', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.insurance_bus_passenger_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('bus_passenger_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('insurance_bus_passenger_id', StringType(), False),
            StructField('insurance_id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.insurance_bus_passengers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('bus_leg_id', StringType(), False),
            StructField('bus_passenger_id', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('insurance_id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('ticket_number', StringType(), True),
            StructField('ticket_url', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.insurance_flight_traveler_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('insurance_flight_traveler_id', StringType(), False),
            StructField('insurance_id', StringType(), False),
            StructField('refund_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.insurance_flight_travelers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('flight_traveler_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('insurance_id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('ticket_number', StringType(), True),
            StructField('ticket_url', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.insurances': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('destiny', StringType(), False),
            StructField('end_date', LongType(), False),
            StructField('fare_type', StringType(), False),
            StructField('id', StringType(), False),
            StructField('is_automatic', LongType(), False),
            StructField('locator', StringType(), True),
            StructField('name', StringType(), False),
            StructField('reference_fare', StringType(), True),
            StructField('reference_insurance', StringType(), True),
            StructField('reference_quote', StringType(), True),
            StructField('start_date', LongType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.log_changes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('field', StringType(), False),
            StructField('from', StringType(), True),
            StructField('id', LongType(), False),
            StructField('log_id', LongType(), True),
            StructField('to', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.logs': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('executor_id', StringType(), False),
            StructField('executor_type', StringType(), False),
            StructField('id', LongType(), False),
            StructField('origin_id', StringType(), False),
            StructField('origin_table', StringType(), False),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.migrations': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('batch', LongType(), False),
            StructField('id', LongType(), False),
            StructField('migration', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.origins': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('type', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', StringType(), True)
        ])
    },

    'corp-app-prod.main_booking.phone_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('phone_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.phones': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('idd', StringType(), False),
            StructField('number', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('amount', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('creator_id', StringType(), False),
            StructField('fee_amount', LongType(), False),
            StructField('id', StringType(), False),
            StructField('refund_mode', StringType(), False),
            StructField('refund_reason', StringType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_additional_charge_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('id', StringType(), False),
            StructField('refund_id', StringType(), False),
            StructField('rental_car_additional_charge_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_additional_charges': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), False),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('status', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_booking_locators': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_locator', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_driver_additional_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), True),
            StructField('rental_car_driver_id', StringType(), False),
            StructField('rental_car_service_id', StringType(), False),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_driver_histories_pivots': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('id', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_drivers': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('customer_id', StringType(), False),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_drivers_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('rental_car_drivers_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_fare_features': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('description', StringType(), True),
            StructField('id', StringType(), False),
            StructField('limited', LongType(), True),
            StructField('price_km_excess', LongType(), True),
            StructField('rental_car_fare_id', StringType(), False),
            StructField('type', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_fare_refund_policies': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('end', LongType(), False),
            StructField('fee', LongType(), False),
            StructField('id', StringType(), False),
            StructField('rental_car_fare_id', StringType(), False),
            StructField('start', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_fare_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('rental_car_fare_id', StringType(), False),
            StructField('rental_car_service_id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_fare_taxes': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('price', LongType(), False),
            StructField('rental_car_fare_id', StringType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_fares': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('total_daily', LongType(), False),
            StructField('travel_entity_id', LongType(), True),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_car_histories': {
        "primary_keys": ['rental_car_id','history_id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('rental_car_id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_refunds': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('id', LongType(), False),
            StructField('refund_id', StringType(), False),
            StructField('rental_car_id', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_schedule_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('rental_car_schedule_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_schedules': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('car_rental', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('deposit_agency', StringType(), False),
            StructField('deposit_date', LongType(), False),
            StructField('id', StringType(), False),
            StructField('rental_car_id', LongType(), False),
            StructField('updated_at', LongType(), True),
            StructField('withdraw_agency', StringType(), False),
            StructField('withdraw_date', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_services': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('code', StringType(), False),
            StructField("created_at", LongType(), True),
            StructField('eligible', LongType(), False),
            StructField('id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', LongType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_vehicle_characteristic_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('rental_car_vehicle_characteristic_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_vehicle_characteristics': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('deleted_at', LongType(), True),
            StructField('id', StringType(), False),
            StructField('key', StringType(), True),
            StructField('rental_car_vehicle_id', StringType(), False),
            StructField('updated_at', LongType(), True),
            StructField('value', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_vehicle_histories': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('history_id', StringType(), False),
            StructField('id', LongType(), False),
            StructField('rental_car_vehicle_id', StringType(), False)
        ])
    },

    'corp-app-prod.main_booking.rental_car_vehicles': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField("created_at", LongType(), True),
            StructField('group', StringType(), False),
            StructField('group_description', StringType(), False),
            StructField('id', StringType(), False),
            StructField('image_url', StringType(), True),
            StructField('rental_car_id', LongType(), False),
            StructField('updated_at', LongType(), True)
        ])
    },

    'corp-app-prod.main_booking.rental_cars': {
        "primary_keys":  ['id'],
        "schema": StructType([
            StructField('booking_id', LongType(), False),
            StructField("created_at", LongType(), True),
            StructField('id', LongType(), False),
            StructField('old_id', LongType(), True),
            StructField('origin_id', StringType(), False),
            StructField('status', StringType(), True),
            StructField('updated_at', LongType(), True)
        ])
    }
}
