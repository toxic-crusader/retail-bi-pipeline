from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .utils import resolve_project_root


@dataclass(frozen=True)
class PipelineConfig:
    """Хранит централизованные настройки retail-пайплайна.

    В объекте собраны пути проекта, имена исходных файлов и наборы
    бизнес-правил, которые используются в нормализации, классификации
    строк и построении витрин. Конфиг намеренно сделан простым и
    декларативным, чтобы его было легко читать и переиспользовать.
    """

    project_root: Path = field(default_factory=resolve_project_root)
    source_file_name: str = "Retail.xlsx"
    sheet_name: str = "online_retail_data"
    export_csv: bool = True
    export_excel: bool = True
    datalens_workbook_name: str = "retail_datalens_export.xlsx"

    @property
    def data_dir(self) -> Path:
        """Возвращает корневую директорию для всех слоёв данных проекта."""
        return self.project_root / "data"

    @property
    def raw_dir(self) -> Path:
        """Возвращает каталог с сырыми входными данными."""
        return self.data_dir / "raw"

    @property
    def interim_dir(self) -> Path:
        """Возвращает каталог для промежуточных таблиц и аудированного слоя."""
        return self.data_dir / "interim"

    @property
    def processed_dir(self) -> Path:
        """Возвращает каталог для итоговых BI-витрин."""
        return self.data_dir / "processed"

    @property
    def datalens_workbook_path(self) -> Path:
        """Returns the path to the Excel workbook for Yandex DataLens."""
        return self.processed_dir / self.datalens_workbook_name

    @property
    def qa_dir(self) -> Path:
        """Возвращает каталог для QA-таблиц и контрольных сводок."""
        return self.data_dir / "qa"

    @property
    def notebooks_dir(self) -> Path:
        """Возвращает каталог для notebook-артефактов проекта."""
        return self.project_root / "notebooks"

    @property
    def reports_dir(self) -> Path:
        """Возвращает каталог для JSON-сводок и прочих служебных отчётов."""
        return self.project_root / "reports"

    @property
    def source_candidates(self) -> tuple[Path, ...]:
        """Возвращает допустимые расположения исходного файла `Retail.xlsx`.

        Порядок важен: сначала проверяется рекомендованный путь в raw-слое,
        затем совместимый вариант с размещением файла в корне проекта.
        """
        return (
            self.raw_dir / self.source_file_name,
            self.project_root / self.source_file_name,
        )

    @property
    def shipping_codes(self) -> frozenset[str]:
        """Возвращает коды строк, относящихся к доставке и почтовым расходам."""
        return frozenset({"POST", "DOT"})

    @property
    def discount_codes(self) -> frozenset[str]:
        """Возвращает коды строк, которые интерпретируются как скидки."""
        return frozenset({"D"})

    @property
    def manual_adjustment_codes(self) -> frozenset[str]:
        """Возвращает коды ручных корректировок и нетоварных служебных движений."""
        return frozenset({"M", "ADJUST", "S"})

    @property
    def commission_codes(self) -> frozenset[str]:
        """Возвращает коды комиссий, сборов и прочих финансовых удержаний."""
        return frozenset({"AMAZONFEE", "CRUK", "BANK CHARGES"})

    @property
    def test_codes(self) -> frozenset[str]:
        """Возвращает коды тестовых строк, которые не должны попадать в товарный контур."""
        return frozenset({"TEST001", "TEST002"})

    @property
    def gift_prefix(self) -> str:
        """Возвращает префикс кодов подарочных сертификатов."""
        return "GIFT_"

    @property
    def required_line_types(self) -> tuple[str, ...]:
        """Возвращает обязательный набор классов `line_type` для итоговой модели."""
        return (
            "sale",
            "return",
            "shipping",
            "discount",
            "manual_adjustment",
            "commission_fee",
            "bad_debt",
            "gift_voucher",
            "test",
            "unknown",
        )

    @property
    def country_map(self) -> dict[str, str]:
        """Возвращает словарь нормализации нестандартизованных стран.

        Словарь применяется после базовой текстовой очистки и переводит
        специальные или нестандартные значения в канонический вид,
        пригодный для BI-измерения стран.
        """
        return {
            "EIRE": "Ireland",
            "USA": "United States",
            "RSA": "South Africa",
            "UNSPECIFIED": "Unknown",
            "EUROPEAN COMMUNITY": "European Community / Other",
        }

    @property
    def country_region_map(self) -> dict[str, str]:
        """Возвращает маппинг нормализованных стран к географическим регионам."""
        return {
            "United Kingdom": "UK",
            "France": "Western Europe",
            "Germany": "Western Europe",
            "Belgium": "Western Europe",
            "Netherlands": "Western Europe",
            "Spain": "Western Europe",
            "Portugal": "Western Europe",
            "Italy": "Western Europe",
            "Switzerland": "Western Europe",
            "Austria": "Western Europe",
            "Ireland": "Western Europe",
            "Channel Islands": "Western Europe",
            "Norway": "Scandinavia",
            "Sweden": "Scandinavia",
            "Denmark": "Scandinavia",
            "Finland": "Scandinavia",
            "Iceland": "Scandinavia",
            "Poland": "Eastern Europe",
            "Czech Republic": "Eastern Europe",
            "Lithuania": "Eastern Europe",
            "Cyprus": "Eastern Europe",
            "Malta": "Eastern Europe",
            "Greece": "Eastern Europe",
            "Israel": "Middle East",
            "United Arab Emirates": "Middle East",
            "Bahrain": "Middle East",
            "Saudi Arabia": "Middle East",
            "Lebanon": "Middle East",
            "Japan": "Asia Pacific",
            "Singapore": "Asia Pacific",
            "Hong Kong": "Asia Pacific",
            "Australia": "Asia Pacific",
            "Korea": "Asia Pacific",
            "Thailand": "Asia Pacific",
            "United States": "Americas",
            "Canada": "Americas",
            "Brazil": "Americas",
            "Bermuda": "Americas",
            "West Indies": "Americas",
            "South Africa": "Africa",
            "Nigeria": "Africa",
            "European Community / Other": "Other",
            "Unknown": "Other",
        }

    @property
    def product_category_keywords(self) -> dict[str, list[str]]:
        """Возвращает маппинг категорий товаров к ключевым словам в описании."""
        return {
            "Christmas & Seasonal": [
                "CHRISTMAS", "XMAS", "ADVENT", "SANTA", "SNOWMAN",
                "REINDEER", "STOCKING", "BAUBLE", "HOLLY", "MISTLETOE",
                "EASTER", "HALLOWEEN", "VALENTINE",
            ],
            "Kitchen & Dining": [
                "MUG", "CUP", "PLATE", "BOWL", "SPOON", "FORK", "KNIFE",
                "JUG", "TEAPOT", "BAKING", "CAKE", "COASTER", "PLACEMAT",
                "NAPKIN", "TRAY", "LUNCH", "BOTTLE", "GLASS",
            ],
            "Bags & Accessories": [
                "BAG", "SHOPPER", "TOTE", "PURSE", "WALLET", "BACKPACK",
            ],
            "Home Decor": [
                "CANDLE", "CUSHION", "FRAME", "MIRROR", "VASE", "CLOCK",
                "LAMP", "DOORMAT", "CURTAIN", "BUNTING", "GARLAND",
                "HOOK", "HANGING", "DECORATION", "ORNAMENT", "WREATH",
            ],
            "Stationery": [
                "PEN", "PENCIL", "NOTEBOOK", "CARD", "PAPER", "JOURNAL",
                "DIARY", "BOOKMARK", "LETTER", "WRAP", "RIBBON", "TISSUE",
                "ENVELOPE",
            ],
            "Toys & Games": [
                "TOY", "GAME", "PUZZLE", "DOLL", "TEDDY", "ROBOT",
                "RACING", "TRAIN", "SOLDIER", "PUPPET",
            ],
            "Garden": [
                "GARDEN", "PLANT", "SEED", "GROW", "WATERING", "HERB",
                "FLOWER POT",
            ],
            "Storage & Organization": [
                "BOX", "TIN", "JAR", "BASKET", "CABINET", "CONTAINER",
                "STORAGE", "DRAWER", "HOOK",
            ],
            "Lighting": [
                "LIGHT", "LANTERN", "CANDLE HOLDER", "TEALIGHT", "LED",
                "FAIRY LIGHT",
            ],
            "Textile & Fabric": [
                "TOWEL", "APRON", "BLANKET", "THROW", "RUG", "FABRIC",
                "COTTON", "LINEN", "FELT",
            ],
        }

    @property
    def anonymous_customer_label(self) -> str:
        """Возвращает каноническую метку для анонимных покупателей."""
        return "ANONYMOUS"

    @property
    def unknown_channel_label(self) -> str:
        """Возвращает каноническую метку для неизвестного канала."""
        return "UNKNOWN"

    @property
    def business_key_columns(self) -> tuple[str, ...]:
        """Возвращает набор колонок для поиска дубликатов по бизнес-ключу.

        Это не технический идентификатор строки, а комбинация полей,
        которая должна совпадать у бизнес-дубликатов одной и той же
        транзакционной записи.
        """
        return (
            "Invoice",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "Price",
            "Customer ID",
            "Country",
            "Channel",
        )
