# О скрипте
Данный скрипт позволяет починить сломанные при копировании рабочего места формулы пересчета в работах.

Скрипт смотрит как на название результата, так и на идентифицирующие приписки к результату (по типу "(анализатор)" или "(Пересчет)").

Можно пофиксить все формулы в каждой из работ входящих в одно рабочее место, так и точечно указать в скритпе название работы по которой вы хотите происзвести починку (для этого пропустите шаг с указанием рабочего места).

# Как запустить?
1) Скачиваете или клонируете себе на ноутбук этот репо.
2) Скачиваете актуальную версию интерпретатора питона, я использовал https://www.python.org/downloads/release/python-3110/
3) Запускаете cmd или терминал из той папки где находится репо на вашем компьютере.
4) python -m pip install -r requirements.txt (На данном шаге на вашем ПК должен быть интернет, после этого шага можно обойтись без него)
5) python main.py 

