'''
A wider fan-out/fan-in DAG (many joiners sharing two upstream parents). Every
join runs with nb_tasks=1; the upstream identity processor can still be
replicated. Demonstrates that the distributed engine handles arbitrary DAG
shapes, not just linear chains.

    python examples/simple_mp_example2.py
'''
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def build_flow():
    reader = IntProducer(0, 100, 0.001, name = 'reader')
    game_state = IdentityProcessor(fps = 6, nb_tasks = 1, name = 'game_state')(reader)

    hero = JoinerProcessor(name = 'hero')(reader, game_state)
    ability = JoinerProcessor(name = 'ability')(reader, game_state, hero)
    ammo = JoinerProcessor(name = 'ammo')(reader, game_state)
    death = JoinerProcessor(name = 'death')(reader, game_state)
    hp = JoinerProcessor(name = 'hp')(reader, game_state)
    killfeed = JoinerProcessor(fps = 1, name = 'killfeed')(reader, game_state)
    game_map = JoinerProcessor(name = 'game_map')(reader, game_state)
    resurrect = JoinerProcessor(name = 'resurrect')(reader, game_state)
    sr = JoinerProcessor(name = 'sr')(reader, game_state)
    ultimate = JoinerProcessor(name = 'ultimate')(reader, game_state)
    player_score = JoinerProcessor(name = 'player_score')(reader, game_state)

    consumer_before = JoinerProcessor(name = 'consumer_before')(
        reader, game_state, hero, death, killfeed, ammo, hp, ultimate,
        ability, player_score, game_map, sr, resurrect,
    )
    consumer = CommandlineConsumer(name = 'consumer')(consumer_before)
    return Flow([consumer], flow_type = BATCH)

if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
