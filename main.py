from src.integrator import Integrator

integration = Integrator(term="next")
integration.update_mirror_tables()
integration.update_canvas()

# integration_next_sem = Integrator(term="current")
# integration_next_sem.update_mirror_tables()
# integration_next_sem.update_canvas()

