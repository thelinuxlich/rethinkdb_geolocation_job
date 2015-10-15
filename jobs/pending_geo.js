"use strict";

module.exports = function(r) {
    // the final step on each pending session iteration
    const update_session_data = function(session, geo, weather) {
        return r.table("sessions").get(session("old_val")("id")).update({
            geo: geo,
            point: r.point(geo("longitude"), geo("latitude")),
            weather: r.branch(
                weather.ne(null), {
                    type: weather("type"),
                    temperature: weather("temperature"),
                    weather_icon: weather("weather_icon")
                },
                null
            )
        });
    };

    const get_weather_from_api = function(session, geo) {
        const API_URL = r("http://api.openweathermap.org/data/2.5/weather?lat=")
            .add(geo("latitude").coerceTo("string")).add("&lon=")
            .add(geo("longitude").coerceTo("string"))
            .add("&units=metric&APPID=YOUR_API_ID"); // this will run server-side so we can't rely on Javascript native concatenation syntax

        return r.http(API_URL).default(null).do(function(weather) { // in case there is a error we will return null from the HTTP request
            return r.branch(
                weather.typeOf().eq("OBJECT").and(weather("weather").ne(null)), // validating the API response format
                r.table("cached_weathers").insert({
                    id: geo("longitude").coerceTo("string").add(",").add(geo("latitude").coerceTo("string")),
                    type: weather("weather").nth(0)("main"),
                    temperature: weather("main")("temp"),
                    weather_icon: weather("weather").nth(0)("icon"),
                    expires_at: r.now().add(10800) // 3-hour cache
                }, {
                    returnChanges: true
                })("changes").nth(0)("new_val").without("expires_at"), // return weather data without the expiration field
                null
            ).do(function(weather) {
                return update_session_data(session, geo, weather); // finally update session data with geolocation and weather
            });
        });
    };

    const get_weather_from_cache = function(session, geo) {
        const weather_id = geo("longitude").coerceTo("string")
            .add(",").add(geo("latitude").coerceTo("string"));
        return r.branch(
            geo.ne(null), // get weather only if we got geolocation coordinates
            r.table("cached_weathers").get(weather_id).do(function(weather) {
                return r.branch(
                    weather.eq(null), // no cached weather data
                    get_weather_from_api(session, geo),
                    update_session_data(session, geo, weather)
                );
            }), [] // you may ask why we are returning a empty array instead of null, well, forEach will be called on the pending sessions array and it will throw an error if we return null, as of RethinkDB 2.0
        );
    };

    const geo_from_api = function(session) {
        const TELIZE_URL = r("http://www.telize.com/geoip/")
            .add(session("old_val")("ip")); // this will run server-side so we can't rely on Javascript native concatenation syntax
        // RethinkDB supports sending HTTP requests via r.http
        return r.http(TELIZE_URL).default(null).do(function(data) {
            return r.branch(
                data.ne(null).and(data.hasFields("latitude")), // validating API response
                r.table("cached_geolocations").insert({
                    id: session("old_val")("ip"),
                    country: data("country").default(""),
                    longitude: data("longitude"),
                    latitude: data("latitude"),
                    region: data("region").default(""),
                    region_code: data("region_code").default(""),
                    country_code: data("country_code").default(""),
                    city: data("city").default(""),
                    expires_at: r.now().add(86400) // 1-day cache
                }, {
                    returnChanges: true
                })("changes").nth(0)("new_val").without("expires_at"), // return the cached data without the expiration field 
                null // no valid response
            );
        });
    };

    const geo_from_cache = function(session) {
        return r.table("cached_geolocations").get(session("old_val")("ip"))
            .do(function(geo) {
                return r.branch(
                    geo.eq(null), // no cache
                    geo_from_api(session), // get from Telize API
                    {
                        country: geo("country"),
                        longitude: geo("longitude"),
                        latitude: geo("latitude"),
                        region: geo("region"),
                        region_code: geo("region_code"),
                        country_code: geo("country_code"),
                        city: geo("city")
                    } // otherwise return from our cache table
                );
            });
    };

    const geolocate_session = function(session) {
        // this is the conditional syntax of RethinkDB, r.branch(condition, true, false)
        return r.branch(
            session("old_val").ne(null), // you may think that every change should return a old val, but if two workers are atomically deleting from the job table, there is a chance someone will get a {new_val: null, old_val: null} as of current RethinkDB version
            geo_from_cache(session),
            null // no geolocation was found
        ).do(function(geo) {
            return get_weather_from_cache(session, geo); // get weather data(and update session information)
        });
    };

    const pending_sessions = r.table("pending_geo_sessions").delete({
        returnChanges: true
    })("changes").default([]);

    let busy = false;

    return function() {
        if (!busy) {
            busy = true;
            pending_sessions.do(function(data) {
                return data.forEach(geolocate_session);
            }).run();
            busy = false;
        }
    };
};
