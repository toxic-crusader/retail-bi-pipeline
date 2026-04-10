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
            "cancelled_sale",
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
        """Категория товара → список ключевых слов для классификации по описанию.

        Порядок категорий задаёт приоритет: более специфичные категории
        (Lighting, Christmas, Toys) проверяются раньше общих (Home Decor,
        Kitchen & Dining). Это нужно, потому что слова вроде CANDLE или
        HOOK встречаются и в общих категориях, но если в имени товара есть
        более конкретный маркер (T-LIGHT HOLDER, TEDDY), приоритет
        должен отдаваться ему.
        """
        return {
            "Christmas & Seasonal": [
                "CHRISTMAS", "XMAS", "ADVENT", "SANTA", "SNOWMAN", "SNOWMEN",
                "REINDEER", "STOCKING", "BAUBLE", "BAUBLES", "HOLLY",
                "MISTLETOE", "EASTER", "HALLOWEEN", "VALENTINE", "NATIVITY",
                "NOEL", "YULE",
            ],
            "Lighting": [
                "LIGHT", "LIGHTS", "LAMP", "LAMPS", "LANTERN", "LANTERNS",
                "TEALIGHT", "TEALIGHTS", "T-LIGHT", "LED", "CHANDELIER",
                "NIGHTLIGHT", "NITELITE", "FAIRY LIGHT", "FAIRY LIGHTS",
                "T-LIGHT HOLDER", "CANDLE HOLDER",
            ],
            "Toys & Games": [
                "TOY", "TOYS", "GAME", "GAMES", "PUZZLE", "PUZZLES",
                "DOLL", "DOLLS", "DOLLY", "TEDDY", "TEDDIES", "ROBOT",
                "ROBOTS", "RACING", "TRAIN", "TRAINS", "SOLDIER",
                "SOLDIERS", "PUPPET", "PUPPETS", "CHILDRENS", "KIDS",
                "COLOURING", "BLOCKS", "RATTLE", "SPINNING TOP",
                "SKIPPING", "PLAYING CARDS", "JIGSAW",
            ],
            "Kitchen & Dining": [
                "MUG", "MUGS", "CUP", "CUPS", "PLATE", "PLATES", "BOWL",
                "BOWLS", "SPOON", "SPOONS", "FORK", "FORKS", "KNIFE",
                "KNIVES", "CUTLERY", "JUG", "JUGS", "TEAPOT", "TEAPOTS",
                "TEACUP", "TEACUPS", "TEASET", "TEA SET", "BAKING", "CAKE",
                "CAKES", "CAKESTAND", "CAKESTANDS", "COASTER", "COASTERS",
                "PLACEMAT", "PLACEMATS", "NAPKIN", "NAPKINS", "TRAY",
                "TRAYS", "LUNCH BAG", "LUNCH BOX", "BOTTLE", "BOTTLES",
                "GLASS", "GLASSES", "SAUCER", "SAUCERS", "CAFETIERE",
                "DINNER", "CHOPPING BOARD", "APRON", "APRONS", "TEA TOWEL",
                "TEA TOWELS", "SPATULA", "WHISK", "LADLE", "SCOOP",
                "GRATER", "COLANDER", "COOK", "WINE GLASS", "EGG CUP",
            ],
            "Bags & Accessories": [
                "BAG", "BAGS", "SHOPPER", "SHOPPERS", "TOTE", "TOTES",
                "PURSE", "PURSES", "WALLET", "WALLETS", "BACKPACK",
                "BACKPACKS", "HANDBAG", "HANDBAGS", "UMBRELLA", "UMBRELLAS",
                "PARASOL", "PARASOLS", "SCARF", "SCARVES", "HAT", "HATS",
                "GLOVES", "KEYRING", "KEYRINGS", "KEY RING",
            ],
            "Stationery": [
                "PEN", "PENS", "PENCIL", "PENCILS", "NOTEBOOK", "NOTEBOOKS",
                "NOTE BOOK", "GREETING CARD", "GIFT CARD", "POSTCARD",
                "JOURNAL", "JOURNALS", "DIARY", "DIARIES", "BOOKMARK",
                "BOOKMARKS", "LETTER", "LETTERS", "GIFT WRAP", "GIFTWRAP",
                "WRAPPING", "RIBBON", "RIBBONS", "TISSUE PAPER", "ENVELOPE",
                "ENVELOPES", "STICKER", "STICKERS", "STAMP", "STAMPS",
                "TAGS", "GIFT TAG", "RUBBER", "ERASER", "SHARPENER",
                "PAPER CHAIN", "NOTE PAD",
            ],
            "Home Decor": [
                "CANDLE", "CANDLES", "CUSHION", "CUSHIONS", "FRAME",
                "FRAMES", "MIRROR", "MIRRORS", "VASE", "VASES", "CLOCK",
                "CLOCKS", "DOORMAT", "CURTAIN", "CURTAINS", "BUNTING",
                "GARLAND", "HOOK", "HOOKS", "HANGING", "DECORATION",
                "DECORATIONS", "ORNAMENT", "ORNAMENTS", "WREATH", "WREATHS",
                "SIGN", "SIGNS", "METAL SIGN", "WOODEN SIGN", "CHALKBOARD",
                "BLACKBOARD", "MEMOBOARD", "PEG", "PEGS", "WICKER", "HEART",
                "HEARTS", "DOORSTOP", "DOOR STOP", "HOLDER", "HOLDERS",
                "WALL", "ARTWORK", "PICTURE", "PICTURES", "STATUE",
                "FIGURINE",
            ],
            "Storage & Organization": [
                "BOX", "BOXES", "TIN", "TINS", "JAR", "JARS", "BASKET",
                "BASKETS", "CABINET", "CABINETS", "CONTAINER", "CONTAINERS",
                "STORAGE", "DRAWER", "DRAWERS", "CHEST", "CRATE", "CRATES",
                "HAMPER", "TRUNK", "BIN", "BINS", "POT", "POTS", "CANISTER",
                "CANISTERS",
            ],
            "Garden": [
                "GARDEN", "PLANT", "PLANTS", "SEED", "SEEDS", "GROW",
                "GROWING", "WATERING", "HERB", "HERBS", "FLOWER POT",
                "FLOWERPOT", "TROWEL", "SPADE", "GNOME", "GREENHOUSE",
                "BIRD FEEDER", "BIRDHOUSE", "WIND CHIME", "WINDCHIME",
            ],
            "Textile & Fabric": [
                "TOWEL", "TOWELS", "BLANKET", "BLANKETS", "THROW", "THROWS",
                "RUG", "RUGS", "FABRIC", "FABRICS", "COTTON", "LINEN",
                "FELT", "KNITTED", "WOOL", "WOOLLY", "QUILT", "QUILTS",
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
