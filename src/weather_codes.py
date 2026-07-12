from dataclasses import dataclass


@dataclass(frozen=True)
class WeatherCode:
    code: int
    description: str


WEATHER_CODES: list[WeatherCode] = [
    WeatherCode(0,  "Clear sky"),
    WeatherCode(1,  "Mainly clear"),
    WeatherCode(2,  "Partly cloudy"),
    WeatherCode(3,  "Overcast"),
    WeatherCode(45, "Fog"),
    WeatherCode(48, "Depositing rime fog"),
    WeatherCode(51, "Drizzle: Light"),
    WeatherCode(53, "Drizzle: Moderate"),
    WeatherCode(55, "Drizzle: Dense"),
    WeatherCode(56, "Freezing drizzle: Light"),
    WeatherCode(57, "Freezing drizzle: Dense"),
    WeatherCode(61, "Rain: Slight"),
    WeatherCode(63, "Rain: Moderate"),
    WeatherCode(65, "Rain: Heavy"),
    WeatherCode(66, "Freezing rain: Light"),
    WeatherCode(67, "Freezing rain: Heavy"),
    WeatherCode(71, "Snowfall: Slight"),
    WeatherCode(73, "Snowfall: Moderate"),
    WeatherCode(75, "Snowfall: Heavy"),
    WeatherCode(77, "Snow grains"),
    WeatherCode(80, "Rain showers: Slight"),
    WeatherCode(81, "Rain showers: Moderate"),
    WeatherCode(82, "Rain showers: Violent"),
    WeatherCode(85, "Snow showers: Slight"),
    WeatherCode(86, "Snow showers: Heavy"),
    WeatherCode(95, "Thunderstorm: Slight or moderate"),
    WeatherCode(96, "Thunderstorm with slight hail"),
    WeatherCode(99, "Thunderstorm with heavy hail"),
]
