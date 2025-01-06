# Importe
from typing import Any, List, Tuple, Union, Callable
from dataset import DataSetInterface, DataSetItem

# Klasse DataSet das von DataSetInterface erbt
class DataSet(DataSetInterface):
    def __init__(self, items=[]):
        self.items = items  
        super().__init__(items)
        
    def __setitem__(self, name, id_content):
        self += DataSetItem(name, id_content[0], id_content[1])

    def __iadd__(self, item):
        """
			Hinzufuegen eines DataSetItems

            Args:
                - item (DataSetItem): Das zu hinzufügende Item
			
			Returns:
			    DataSet
		"""
        for index in range(len(self.items)):
            if self.items[index].name == item.name:
                self.items[index] = item
                return self
        existing_items = self.items
        self.items = existing_items + [item]
        return self


    def __delitem__(self, name):
        """
			Loeschen eines Datum.

			Args:
				- name (str): Name des zu loeschenden DataSetItem
		"""
        current_index = 0
        while current_index < len(self.items):
            if self.items[current_index].name == name:
                self.items.pop(current_index)
            current_index += 1


    def __contains__(self, name):
        """
			Pruefung ob ein DataSetItem im DataSet 

			Args:
				- name (str): Name des gesuchten DataSetItem

			Returns:
				bool
		"""
        found = False
        for item in self.items:
            if item.name == name:
                found = True
        return found

    def __getitem__(self, name):
        """
			Abrufen eines DataSetItem

			Args:
				- name (str): Name des gewuenschten DataSetItem

			Returns:
				DataSetItem
		"""
        for item in self.items:
            if item.name == name:
                return item

    def __and__(self, dataset):
        """
			Schnittmenge zwischen zwei DataSets erzeugen.
			Es werden die Namen der DataSetItems als Schluessel genutzt.
			
			Args:
				- dataset (DataSetInterface): Das andere DataSet 

			Returns:
				Neues DataSet, nur mit den DataSetItems im Schnitt
		"""
        intersection = DataSet([])
        for item_self in self.items:
            for item_other in dataset:
                if item_self.name == item_other.name:
                    intersection += item_self
        return intersection

    def __or__(self, dataset):
        """
			Vereinigung von zwei DataSets erzeugen.
			Es werden die Namen der DataSetItems als Schluessel genutzt.

			Args:
				- dataset (DataSetInterface): Das andere DataSet 
				
			Returns:
				Neuen Datensatz, mit allen DataSetItems in beiden DataSets (=der Vereinigung)

		"""
        union = self
        for item in dataset:
            union += item
        return union


    def __iter__(self):
        """
			Iteration ueber die DataSetItems des DataSets.

			Dabei ist die Sortierung nach den Attributen 
				`self.iterate_sorted` 
				`self.iterate_reversed`
				`self.iterate_key`
			zu beachten.
		"""
        for item in self.filtered_iterate(lambda name, item_id: True):
            yield item

    def filtered_iterate(self, filter):
        """
			Iteration ueber die DataSetItems des DataSets.
			Dabei koennen die DataSetItems gefiltert werden.

			Args:
				- filter (callable): z.B. eine Lambda-Funktion mit zwei Parametern.
					Die Funktion bekommt Name und ID jedes DataSetItems und das DataSetItem
					wird nur ausgegeben, wenn die Funktion `True` zurueck gibt.
		"""
        filtered_items = self.items
        if self.iterate_sorted and self.iterate_key == self.ITERATE_SORT_BY_NAME:
            for outer_index in range(len(filtered_items)):
                min_index = outer_index
                for inner_index in range(outer_index, len(filtered_items)):
                    if filtered_items[inner_index].name < filtered_items[min_index].name:
                        min_index = inner_index
                temp_item = filtered_items[outer_index]
                filtered_items[outer_index] = filtered_items[min_index]
                filtered_items[min_index] = temp_item
        elif self.iterate_sorted and self.iterate_key == self.ITERATE_SORT_BY_ID:
            for outer_index in range(len(filtered_items)):
                min_index = outer_index
                for inner_index in range(outer_index, len(filtered_items)):
                    if filtered_items[inner_index].id < filtered_items[min_index].id:
                        min_index = inner_index
                temp_item = filtered_items[outer_index]
                filtered_items[outer_index] = filtered_items[min_index]
                filtered_items[min_index] = temp_item

        if self.iterate_reversed:
            filtered_items.reverse()

        for item in filtered_items:
            if filter(item.name, item.id):
                yield item

    def __len__(self):
        """
			Laenge des Objekts bestimmen.

			Returns:
				Anzahl der DataSetItems im DataSet
		"""
        return len(self.items)