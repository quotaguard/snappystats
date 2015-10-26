package snappystats

import (
	"time"
	"math"
	"strings"
	"strconv"
	"github.com/garyburd/redigo/redis"
	//"log"
	)

type Granularity struct{
	size float64
	ttl float64
	factor float64
	name string
}

var minute = Granularity{size:1440, ttl: 172800, factor: 60, name: "minute"} // Minute
var hour = Granularity{size:168, ttl: 1209600, factor: 3600, name: "hour"} // Hour
var day = Granularity{size:365, ttl: 63113880, factor: 86400, name: "day"} // Day

func getSecondsTimestamp() float64 {
 return float64(time.Now().UTC().Unix())
}


func getRoundedTimestamp( ts float64, precision float64 ) float64{
  return math.Floor( ts / precision ) * precision
}

func getFactoredTimestamp( ts_seconds float64, factor float64 ) float64{
  return math.Floor( ts_seconds / factor ) * factor
}

func RecordHitNow(key string, c redis.Conn){
  RecordHit(getSecondsTimestamp(), key, c)
}

func RecordHit( time float64, key string, c redis.Conn ) bool{	
	recordHitAtGranularity(minute, time, key, c)
	recordHitAtGranularity(hour, time, key, c)
	recordHitAtGranularity(day, time, key, c)
    return true;
}

func recordHitAtGranularity( granularity Granularity, time float64, key string, c redis.Conn ) bool{
	size := granularity.size
	factor := granularity.factor
	ttl := granularity.ttl
	name := granularity.name
	tsround := getRoundedTimestamp(time, size * factor)
	s := []string{"stats",key,name,strconv.FormatFloat(tsround, 'f', 0, 64)}
	redis_key := strings.Join(s,":")
	ts := strconv.FormatFloat(getFactoredTimestamp (time, factor), 'f', 0, 64)    	
	ttl_str := strconv.FormatFloat(tsround+ttl, 'f', 0, 64)
	c.Send("HINCRBY", redis_key, ts, 1)	
	c.Send("EXPIREAT", redis_key, ttl_str)	
	return true;
}