def calc_margin(selling_cost: float, standard_cost: float) -> float:
    if selling_cost and selling_cost > 0:
        return round((selling_cost - standard_cost) / selling_cost * 100, 2)
    return 0.0
