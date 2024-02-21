const report_root = "pdf"

const container = document.querySelector('#container');

// Read data from #dataframe table
const table = document.getElementById("dataframe");
let of_cols_idx = [];

const cols_length = table.rows[0].cells.length;
let cols = [];
for (let i = 0; i < cols_length; i++) {
    let value = table.rows[0].cells[i].innerHTML;
    let type = 'text'
    if (value == "idx" || value.includes("age") || value.includes("day") || value.includes("openface") || value.includes("score")) {
        type = 'numeric'
    }
    if (value.includes("openface")) {
        of_cols_idx.push(i);
    }
    if (value.includes("duration") || value.includes("time")) {
        type = 'time'
    }
    if (value.includes("interview_status")) {
        cols.push({
            title: value,
            type: type,
            data: value,
            renderer: 'html'
        });
        continue;
    }
    cols.push({
        title: value,
        type: type,
        data: value,
    });
}

required_cols = [];

required_cols.push({
    title: "subject_id",
    data: "subject_id"
});
required_cols.push({
    title: "visit_day",
    data: "day"
});
required_cols.push({
    title: "interview_status",
    data: "interview_status"
});
required_cols.push({
    title: "diagnosis",
    data: "demographics.diagnosis"
});
required_cols.push({
    title: "age",
    data: "demographics.age"
});
required_cols.push({
    title: "gender",
    data: "demographics.gender"
});
required_cols.push({
    title: "race",
    data: "demographics.race"
});
required_cols.push({
    title: "duration",
    data: "openface_metrics.duration"
});
required_cols.push({
    title: "openfaceQCConfidence",
    data: "openface_metrics.qc.successful_frames_confidence_mean"
});
required_cols.push({
    title: "openfaceQCFrames",
    data: "openface_metrics.qc.sucessful_frames_percentage"
});
required_cols.push({
    title: "mcas",
    data: "clinical_scores.mcas"
});
required_cols.push({
    title: "ymrs",
    data: "clinical_scores.ymrs"
});
required_cols.push({
    title: "madrs",
    data: "clinical_scores.madrs"
});
required_cols.push({
    title: "panss_general",
    data: "clinical_scores.panss_general"
});
required_cols.push({
    title: "panss_negative",
    data: "clinical_scores.panss_negative"
});
required_cols.push({
    title: "panss_positive",
    data: "clinical_scores.panss_positive"
});

nestedHeaders = [
    [{ label: '', colspan: 10 }, { label: 'clinical values', colspan: 7 }],
    [{ label: 'metadata', colspan: 3 }, { label: 'demographics', colspan: 4 }, { label: 'OpenFaceQC', colspan: 3 }, { label: '', colspan: 3 }, { label: 'panss', colspan: 3 }],
    ['subject_id', 'visitDay', 'report', 'diagnosis', 'age', 'gender', 'race', 'duration', 'confidence', 'frames', 'mcas', 'ymrs', 'madrs', 'general', 'negative', 'positive']
];

// Map required_cols to cols
for (let i = 0; i < required_cols.length; i++) {
    let r_col = required_cols[i];
    let idx = cols.findIndex(x => x.data == r_col.data);

    if (idx == -1) {
        console.log("required column not found: " + r_col.data);
        continue;
    }

    col = cols[idx];

    if (col.renderer) {
        r_col.renderer = col.renderer;
    }
    if (col.type) {
        r_col.type = col.type;
    }
}

console.log(required_cols);

let data = [];
for (let i = 1; i < table.rows.length; i++) {
    let row = {}
    for (let j = 0; j < table.rows[i].cells.length; j++) {
        let col = cols[j].title;
        if (col == "")
            col = "idx"
        let value = table.rows[i].cells[j].innerHTML;
        if (value == "NaN")
            value = '';
        if (value == "Prefer not to answer / Unknown")
            value = '';
        if (col.includes("interview_status")) {
            if (value.length > 6) {
                value = ""
            } else {
                raw_pdf_path = table.rows[i].cells[table.rows[i].cells.length - 1].innerHTML
                pdf_name = raw_pdf_path.split("/").pop()
                value = "<a href='" + report_root + "/" + pdf_name + "' target='_blank'>" + value + "</a>";
            }
        }
        row[col] = value;
    }
    data.push(row);
}

console.log(data[0]);

function emptyCellFormatter(instance, td, row, col, prop, value, cellProperties) {
    Handsontable.renderers.TextRenderer.apply(this, arguments);

    if (!value || value === '') {
        td.style.background = '#EEE';

    } else {
        td.style.background = '';
    }
}

function scoresCellFormatter(instance, td, row, col, prop, value, cellProperties) {
    Handsontable.renderers.TextRenderer.apply(this, arguments);

    const maxValue = 60;
    const numCategories = 5;

    const categoryWidth = maxValue / numCategories;

    const category = Math.floor(value / categoryWidth);

    if (!value) {
        td.style.background = '#EEE';
    } else {
        if (category >= 4) {
            td.style.background = '#EE2E29';
            td.style.color = '#FFFFFF';
        }
        else if (category == 3) {
            td.style.background = '#FFCF31';
            td.style.color = '#000000';
        }
        else if (category == 2) {
            td.style.background = '#A3D06D';
            td.style.color = '#000000';
        }
        else if (category == 1) {
            td.style.background = '#5AC9EB';
            td.style.color = '#000000';
        }
        else if (category == 0) {
            td.style.background = '#3E59A8';
            td.style.color = '#FFFFFF';
        }
    }
}

function percentageCellFormatter(instance, td, row, col, prop, value, cellProperties) {
    Handsontable.renderers.TextRenderer.apply(this, arguments);

    if (!value) {
        td.style.background = '#EEE';
    } else {
        if (value > 1) {
            value = value / 100;
        }
        if (value < .9) {
            td.style.color = '#FF0000';
        }
        else if (value < .95) {
            td.style.color = '#FFA500';
        }
    }
}

function durationCellFormatter(instance, td, row, col, prop, value, cellProperties) {
    Handsontable.renderers.TextRenderer.apply(this, arguments);

    if (!value) {
        td.style.background = '#EEE';
    } else {
        if (value > '01:00:00') {
            td.style.background = '#EE2E29';
            td.style.color = '#FFFFFF';
        }
        else if (value > '00:40:00') {
            td.style.background = '#FFCF31';
            td.style.color = '#000000';
        }
        else if (value > '00:20:00') {
            td.style.background = '#A3D06D';
            td.style.color = '#000000';
        }
        else {
            td.style.background = '#3E59A8';
            td.style.color = '#FFFFFF';
        }
    }
}

Handsontable.renderers.registerRenderer('emptyCellFormatter', emptyCellFormatter);
Handsontable.renderers.registerRenderer('scoresCellFormatter', scoresCellFormatter);
Handsontable.renderers.registerRenderer('percentageCellFormatter', percentageCellFormatter);
Handsontable.renderers.registerRenderer('durationCellFormatter', durationCellFormatter);

var col_widths = [];
const default_col_width = 100
for (var i = 1; i <= required_cols.length; i++) {
    col_widths.push(default_col_width);
}

SUBJECT_ID = 0
VISIT_DAY = 1
INTERVIEW_STATUS = 2
DIAGNOSIS = 3
AGE = 4
GENDER = 5
RACE = 6
DURATION = 7
CONFIDENCE = 8
FRAMES = 9
MCAS = 10
YMRS = 11
MADRS = 12
PANSS_GENERAL = 13
PANSS_NEGATIVE = 14
PANSS_POSITIVE = 15

col_widths[VISIT_DAY] = 50
col_widths[DIAGNOSIS] = 200
col_widths[AGE] = 50
col_widths[GENDER] = 50
col_widths[RACE] = 150

const hot = new Handsontable(container, {
    data: data,
    readOnly: true,
    columns: required_cols,
    nestedHeaders: nestedHeaders,
    rowHeaders: true,
    colWidths: col_widths,
    manualColumnResize: true,
    manualColumnFreeze: true,
    multiColumnSorting: {
        initialConfig: [
            {
                column: 0,
                sortOrder: 'asc',
            },
            {
                column: 1,
                sortOrder: 'asc',
            },
        ],
    },
    // columnSorting: true,
    filters: true,
    dropdownMenu: true,
    manualColumnMove: true,
    contextMenu: true,
    renderer: 'html',
    hiddenColumns: {
        // columns: [0],
        indicators: true,
    },
    cells(row, col) {
        let cellProperties = {};
        const data = this.instance.getData();

        if (col != INTERVIEW_STATUS && col != required_cols.length - 1) {
            cellProperties.renderer = 'emptyCellFormatter';
        }
        if (col == DURATION) {
            cellProperties.renderer = 'durationCellFormatter';
        }
        if (col == MCAS || col == YMRS || col == MADRS || col == PANSS_GENERAL || col == PANSS_NEGATIVE || col == PANSS_POSITIVE) {
            cellProperties.renderer = 'scoresCellFormatter';
        }
        if (col == FRAMES) {
            cellProperties.renderer = 'percentageCellFormatter';
        }

        return cellProperties;
    },
    licenseKey: 'non-commercial-and-evaluation'
});