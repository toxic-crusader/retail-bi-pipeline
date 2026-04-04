"""Централизованная конфигурация pipeline: пути, бизнес-правила, справочники."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .utils import resolve_project_root


@dataclass(frozen=True)
class PipelineConfig:
    """Настройки retail-пайплайна: пути проекта, имена файлов, бизнес-правила.

    Все параметры — декларативные константы. Код трансформации ссылается
    на них через ``cfg``, не хардкодя значения внутри модулей.
    """

    project_root: Path = field(default_factory=resolve_project_root)
    source_file_name: str = "Retail.xlsx"
    sheet_name: str = "online_retail_data"
    export_csv: bool = True
    export_excel: bool = True
    datalens_workbook_name: str = "retail_datalens_export.xlsx"

    # --- Пути проекта ---------------------------------------------------

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def interim_dir(self) -> Path:
        return self.data_dir / "interim"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def datalens_workbook_path(self) -> Path:
        return self.processed_dir / self.datalens_workbook_name

    @property
    def qa_dir(self) -> Path:
        return self.data_dir / "qa"

    @property
    def notebooks_dir(self) -> Path:
        return self.project_root / "notebooks"

    @property
    def reports_dir(self) -> Path:
        return self.project_root / "reports"

    @property
    def source_candidates(self) -> tuple[Path, ...]:
        """Допустимые расположения исходного файла (raw-слой, затем корень)."""
        return (
            self.raw_dir / self.source_file_name,
            self.project_root / self.source_file_name,
        )

    # --- Коды служебных строк -------------------------------------------

    @property
    def shipping_codes(self) -> frozenset[str]:
        return frozenset({"POST", "DOT"})

    @property
    def discount_codes(self) -> frozenset[str]:
        return frozenset({"D"})

    @property
    def manual_adjustment_codes(self) -> frozenset[str]:
        return frozenset({"M", "ADJUST", "S"})

    @property
    def commission_codes(self) -> frozenset[str]:
        return frozenset({"AMAZONFEE", "CRUK", "BANK CHARGES"})

    @property
    def test_codes(self) -> frozenset[str]:
        return frozenset({"TEST001", "TEST002"})

    @property
    def gift_prefix(self) -> str:
        return "GIFT_"

    @property
    def required_line_types(self) -> tuple[str, ...]:
        """Полный набор классов ``line_type``, ожидаемый в итоговой модели."""
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

    # --- Справочники нормализации ----------------------------------------

    @property
    def country_map(self) -> dict[str, str]:
        """Нестандартные названия стран → канонические (применяется после trim/upper)."""
        return {
            "EIRE": "Ireland",
            "USA": "United States",
            "RSA": "South Africa",
            "UNSPECIFIED": "Unknown",
            "EUROPEAN COMMUNITY": "European Community / Other",
        }

    @property
    def country_region_map(self) -> dict[str, str]:
        """Нормализованная страна → географический регион для dim_country."""
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
        """Категория товара → список ключевых слов для классификации по описанию."""
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

    # --- Метки и ключи ---------------------------------------------------

    @property
    def anonymous_customer_label(self) -> str:
        return "ANONYMOUS"

    @property
    def unknown_channel_label(self) -> str:
        return "UNKNOWN"

    @property
    def business_key_columns(self) -> tuple[str, ...]:
        """Набор полей для поиска дубликатов по бизнес-ключу (не по всей строке)."""
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
