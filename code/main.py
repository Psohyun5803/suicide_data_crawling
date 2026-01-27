import yaml
from collectors import cpi,loan,labor_force,consumer_price_change_index,average_working_day,gdp_gni,aver_mid_age,working_index,resident_population

def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    collectors = [
        ("cpi", cpi.run),
        ("loan", loan.run),
        ("labor_force", labor_force.run),
        ("consumer_price_change_index", consumer_price_change_index.run),
        ("average_working_day", average_working_day.run),
        ("gdp_gni", gdp_gni.run),
        ("aver_mid_age", aver_mid_age.run),
        ("working_index", working_index.run),
        ("resident_population",resident_population.run )
    ]


    for name, fn in collectors:
        cfg = config["collectors"].get(name, {})
        fn(cfg)

if __name__ == "__main__":
    main()