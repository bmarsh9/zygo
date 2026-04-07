class listToBadge {
  eGui;

  // init method gets the details of the cell to be renderer
  init(params) {
    this.eGui = document.createElement('div');
    this.eGui.classList.add("flex", "flex-row", "gap-x-1", "mt-3");

    for (let word of params.value) {
        const newDiv = document.createElement('div');
        newDiv.classList.add('badge', 'badge-ghost', "text-xs", "font-semibold");
        newDiv.textContent = word;
        this.eGui.appendChild(newDiv);
    }

  }

  getGui() {
    return this.eGui;
  }

  refresh(params) {
    return false;
  }
}

function capitalizationRenderer(params) {
    if (!params.value) return '';
    const capitalizedValue = params.value.charAt(0).toUpperCase() + params.value.slice(1).toLowerCase();
    return `<span>${capitalizedValue}</span>`;
}

function dateFormatter(params) {
    if (!params.value) return '';
    const date = new Date(params.value);
    return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    })
}

function filterableCell(params) {
    const rawValue = params.value || '';
    const field = params.colDef.field;

    // Apply the same capitalization transformation
    let displayValue = rawValue;

    return `
        <div class="relative w-full h-full">
            <div class="filterable-cell cursor-pointer hover:bg-base-200 px-2 py-1 rounded flex items-center justify-between group"
                 data-field="${field}"
                 data-value="${rawValue}"
                 onclick="toggleCellFilter(this, '${field}', '${rawValue}')">
                <span class="truncate">${displayValue}</span>
                <svg class="w-3 h-3 opacity-0 group-hover:opacity-100 transition-opacity ml-1 flex-shrink-0"
                     xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                </svg>
            </div>
        </div>
    `;
}

class booleanBadge {
  eGui;

  // init method gets the details of the cell to be renderer
  init(params) {
    this.eGui = document.createElement('div');
    this.eGui.textContent = params.value
    this.eGui.classList.add("opacity-90");

    if (params.value === true) {
        //this.eGui.classList.add("badge-success")
        this.eGui.innerHTML = '<i class="ti ti-progress-check text-success text-lg"></i>'

    } else {
        this.eGui.innerHTML = '<i class="ti ti-progress-x text-error text-lg"></i>'
        //this.eGui.classList.add("badge-error")
    }

  }

  getGui() {
    return this.eGui;
  }

  refresh(params) {
    return false;
  }
}

class idToButton {
  /*
  Given a field with a id, create a button that directs to the next link
  {"text": "Click me", "class": "btn-ghost", "link": "/link/{value}"}
  */
  eGui;

  // init method gets the details of the cell to be renderer
  init(params) {
    this.eGui = document.createElement('div');
    this.eGui.classList.add('flex', 'items-center');

    // Set default values
    const text = params.text || "<i class='ti ti-external-link text-lg'></i>";
    const buttonClass = params.class || "btn-sm btn-ghost";

    if (params.link) {
      // If link is provided, create a link button
      const link = params.link.replace("{value}", params.value);
      this.eGui.innerHTML = `
        <a href='${link}' class='btn ${buttonClass}'>${text}</a>
      `;
    } else {
      // If no link is provided, create a regular button
      this.eGui.innerHTML = `
        <button class='btn ${buttonClass}' onclick="window.tableInstance.openSidebar(${params.node.rowIndex})">${text}</button>
      `;
    }
  }

  getGui() {
    return this.eGui;
  }

  refresh(params) {
    return false;
  }
}

class hasValue {
  eGui;

  // init method gets the details of the cell to be renderer
  init(params) {
    this.eGui = document.createElement('div');
    this.eGui.textContent = params.value
    this.eGui.classList.add("opacity-90");

    if (params.value) {
        //this.eGui.classList.add("badge-success")
        this.eGui.innerHTML = '<i class="ti ti-progress-check text-success text-lg"></i>'

    } else {
        this.eGui.innerHTML = '<i class="ti ti-progress-x text-error text-lg"></i>'
        //this.eGui.classList.add("badge-error")
    }

  }

  getGui() {
    return this.eGui;
  }

  refresh(params) {
    return false;
  }
}