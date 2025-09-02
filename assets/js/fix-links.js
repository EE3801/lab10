document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".nav-list-item a[target='_blank']").forEach(a => {
    a.setAttribute("target", "_self");
  });
});
