from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ninout import Dag


def build_dag() -> Dag:
    dag = Dag()

    @dag.step()
    def extrair() -> None:
        print("Extraindo")
        return "dados brutos"

    @dag.branch(depends_on=[extrair])
    def precisa_transformar() -> bool:
        print("Precisa Transformar")
        return True

    @dag.step(depends_on=[extrair], when=precisa_transformar, condition=True)
    def transformar(results) -> None:
        print("Transformando")
        return results["extrair"].upper()

    @dag.step(depends_on=[extrair], when=precisa_transformar, condition=False)
    def pular_transformacao() -> None:
        print("Pulei a transformação")
        pass

    @dag.step(depends_on=[transformar])
    def carregar(results) -> None:
        print("Carregando os dados no banco")
        print(f"Payload: {results['transformar']}")

    return dag


if __name__ == "__main__":
    dag = build_dag()
    dag.run()
    yaml_path, html_path = dag.to_html(dag_name="example_dag")
    print(f"DAG gerado com {len(dag)} steps em {html_path}")
